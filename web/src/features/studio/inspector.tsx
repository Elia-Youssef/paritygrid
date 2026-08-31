/**
 * Node configuration inspector. Every control is generated from the closed
 * typed node registry — never from arbitrary server keys — with proper
 * labels, descriptions, and field-level validation messages. Unknown or
 * unsafe configuration cannot be expressed here because only registered
 * fields exist. Invalid drafts are preserved so users can correct them.
 */
import { useEffect, useRef } from "react";

import { nodeDefinition } from "../../pipelines/registry";
import type { Diagnostic, PipelineNodeValue } from "../../pipelines/document";

export interface InspectorProps {
  node: PipelineNodeValue | null;
  diagnostics: readonly Diagnostic[];
  onConfigure: (
    nodeId: string,
    field: string | null,
    value: string | number | boolean,
  ) => void;
  onSetConnector: (nodeId: string, connectorId: string | null) => void;
  onClose: () => void;
  connectors: readonly {
    connector_id: string;
    display_name: string;
    capabilities: Readonly<Record<string, boolean>>;
  }[];
  /** Announced after a failed validation attempt to restore focus. */
  focusField: { field: string } | null;
}

function errorsForField(
  diagnostics: readonly Diagnostic[],
  field: string,
): readonly Diagnostic[] {
  return diagnostics.filter((diagnostic) => diagnostic.field === field);
}

export function Inspector(props: InspectorProps) {
  const {
    node,
    diagnostics,
    onConfigure,
    onSetConnector,
    onClose,
    connectors,
    focusField,
  } = props;
  const firstFieldRef = useRef<HTMLInputElement | null>(null);
  const definition = node === null ? undefined : nodeDefinition(node.kind);

  useEffect(() => {
    if (focusField !== null && firstFieldRef.current !== null) {
      firstFieldRef.current.focus();
    }
  }, [focusField]);

  if (node === null || definition === undefined) {
    return (
      <aside aria-label="Node inspector" className="border-l border-border p-4">
        <p className="text-sm text-muted">
          Select a node to inspect its configuration.
        </p>
      </aside>
    );
  }

  const fieldErrors = (name: string) => errorsForField(diagnostics, name);
  const nodeErrors = diagnostics.filter((diagnostic) => diagnostic.nodeId === node.id);
  const connectorErrors = fieldErrors("connector");
  const connectorDescription = [
    "inspector-connector-help",
    connectorErrors.length > 0 ? "inspector-connector-error" : null,
  ]
    .filter((value): value is string => value !== null)
    .join(" ");

  return (
    <aside aria-label="Node inspector" className="border-l border-border p-4">
      <div className="flex items-start justify-between gap-2">
        <div>
          <h3 className="text-sm font-semibold text-foreground">{definition.label}</h3>
          <p className="font-mono text-2xs text-muted">{node.id}</p>
        </div>
        <button
          type="button"
          className="rounded border border-border px-2 py-1 text-2xs text-foreground hover:bg-surface-elevated"
          onClick={onClose}
        >
          Close inspector
        </button>
      </div>
      <p className="mt-1 text-2xs text-muted">{definition.description}</p>

      {nodeErrors.length > 0 && (
        <div
          role="alert"
          className="mt-3 rounded border border-failure/40 bg-failure/10 p-2"
        >
          <p className="text-2xs font-medium text-failure">
            {String(nodeErrors.length)} validation{" "}
            {nodeErrors.length === 1 ? "error" : "errors"}
          </p>
          <ul className="mt-1 list-inside list-disc text-2xs text-failure">
            {nodeErrors.map((diagnostic, index) => (
              <li key={String(index)}>{diagnostic.message}</li>
            ))}
          </ul>
        </div>
      )}

      <form className="mt-3 space-y-3" aria-label={`${definition.label} configuration`}>
        <input type="hidden" aria-hidden="true" ref={firstFieldRef} tabIndex={-1} />
        {definition.connectorRequirement !== "none" && (
          <div>
            <label
              htmlFor="inspector-connector"
              className="block text-xs font-medium text-foreground"
            >
              Connector binding
            </label>
            <p id="inspector-connector-help" className="text-2xs text-muted">
              Required {definition.connectorRequirement} connector
              {definition.requiredCapabilities.length > 0
                ? ` with ${definition.requiredCapabilities.join(" + ")}`
                : ""}
              .
            </p>
            <select
              id="inspector-connector"
              aria-describedby={connectorDescription}
              aria-invalid={connectorErrors.length > 0 || undefined}
              className="mt-1 w-full rounded border border-border bg-surface px-2 py-1.5 text-xs text-foreground"
              value={node.connector_id ?? ""}
              onChange={(event) => {
                onSetConnector(
                  node.id,
                  event.target.value === "" ? null : event.target.value,
                );
              }}
            >
              <option value="">No connector selected</option>
              {connectors.map((connector) => (
                <option key={connector.connector_id} value={connector.connector_id}>
                  {connector.display_name} ({connector.connector_id})
                </option>
              ))}
            </select>
            {connectorErrors.length > 0 && (
              <div id="inspector-connector-error">
                {connectorErrors.map((diagnostic, index) => (
                  <p key={String(index)} className="mt-1 text-2xs text-failure">
                    {diagnostic.message}
                  </p>
                ))}
              </div>
            )}
          </div>
        )}

        {definition.configurationSchema.fields.map((schemaField) => {
          const fieldId = `inspector-${node.id}-${schemaField.name}`;
          const helpId = `${fieldId}-help`;
          const errorId = `${fieldId}-error`;
          const errors = fieldErrors(schemaField.name);
          const describedBy = `${helpId}${errors.length > 0 ? ` ${errorId}` : ""}`;
          const rawValue = node.configuration[schemaField.name];
          return (
            <div key={schemaField.name}>
              <label
                htmlFor={fieldId}
                className="block text-xs font-medium text-foreground"
              >
                {schemaField.name}
                {schemaField.required ? " (required)" : " (optional)"}
              </label>
              <p id={helpId} className="text-2xs text-muted">
                {schemaField.valueKind}
                {schemaField.minimum !== undefined &&
                  ` between ${String(schemaField.minimum)} and ${String(schemaField.maximum)}`}
                .
              </p>
              {schemaField.valueKind === "boolean" ? (
                <select
                  id={fieldId}
                  aria-describedby={describedBy}
                  aria-invalid={errors.length > 0 || undefined}
                  className="mt-1 w-full rounded border border-border bg-surface px-2 py-1.5 text-xs text-foreground"
                  value={
                    typeof rawValue === "boolean" ? (rawValue ? "true" : "false") : ""
                  }
                  onChange={(event) => {
                    if (event.target.value === "") {
                      return;
                    }
                    onConfigure(
                      node.id,
                      schemaField.name,
                      event.target.value === "true",
                    );
                  }}
                >
                  <option value="">Unset</option>
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              ) : (
                <input
                  id={fieldId}
                  aria-describedby={describedBy}
                  aria-invalid={errors.length > 0 || undefined}
                  className="mt-1 w-full rounded border border-border bg-surface px-2 py-1.5 text-xs text-foreground"
                  type={schemaField.valueKind === "integer" ? "number" : "text"}
                  value={rawValue === undefined ? "" : String(rawValue)}
                  onChange={(event) => {
                    const text = event.target.value;
                    if (text === "") {
                      onConfigure(node.id, schemaField.name, "");
                      return;
                    }
                    if (schemaField.valueKind === "integer") {
                      const parsed = Number.parseInt(text, 10);
                      if (!Number.isNaN(parsed)) {
                        onConfigure(node.id, schemaField.name, parsed);
                      }
                      return;
                    }
                    onConfigure(node.id, schemaField.name, text);
                  }}
                />
              )}
              {errors.length > 0 && (
                <div id={errorId}>
                  {errors.map((diagnostic, index) => (
                    <p key={String(index)} className="mt-1 text-2xs text-failure">
                      {diagnostic.message}
                    </p>
                  ))}
                </div>
              )}
            </div>
          );
        })}
        {definition.configurationSchema.fields.length === 0 &&
          definition.connectorRequirement === "none" && (
            <p className="text-2xs text-muted">
              {definition.label} has no configurable fields.
            </p>
          )}
      </form>
    </aside>
  );
}
