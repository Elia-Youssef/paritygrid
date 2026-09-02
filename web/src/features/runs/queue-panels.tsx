/**
 * Queue and saturation panels (P17.4). Every value is advisory telemetry:
 * each panel names its definition, unit, source, and observation timestamp,
 * and an absent metric renders as explicitly unavailable — never zero.
 * Saturation is computed only when depth and a positive capacity are both
 * present in the same telemetry slice, and the whole panel carries the
 * stale/disconnected state of the telemetry channel.
 */
import { DisconnectedNotice } from "../../components/states/disconnected-notice";
import { UnavailableNotice } from "../../components/states/unavailable-notice";
import { StaleNotice } from "../../components/states/stale-notice";
import { formatObservation, type QueuePanelView } from "./run-derivations";

export interface QueuePanelsProps {
  readonly view: QueuePanelView;
}

export function QueuePanels({ view }: QueuePanelsProps) {
  return (
    <div data-testid="queue-panels">
      <div className="grid gap-3 p-4 sm:grid-cols-2 xl:grid-cols-4">
        {view.metrics.map((metric) => (
          <div
            key={metric.name}
            data-testid={`queue-metric-${metric.name.toLowerCase().replace(/[^a-z]+/g, "-")}`}
            className="rounded-lg border border-border p-3"
          >
            <p className="text-2xs uppercase tracking-wide text-muted">{metric.name}</p>
            <p className="mt-1 font-mono text-lg text-foreground">
              {metric.value === null ? (
                <span className="text-sm text-muted">unavailable</span>
              ) : (
                String(metric.value)
              )}
            </p>
            <dl className="mt-2 space-y-1 text-2xs text-muted">
              <div>
                <dt className="inline font-medium text-muted-strong">Definition: </dt>
                <dd className="inline">{metric.definition}</dd>
              </div>
              <div>
                <dt className="inline font-medium text-muted-strong">Unit: </dt>
                <dd className="inline">{metric.unit}</dd>
              </div>
              <div>
                <dt className="inline font-medium text-muted-strong">Source: </dt>
                <dd className="inline font-mono">{metric.source}</dd>
              </div>
              <div>
                <dt className="inline font-medium text-muted-strong">Observed: </dt>
                <dd className="inline font-mono" data-testid="queue-observed-at">
                  {formatObservation(metric.observedMicros)}
                </dd>
              </div>
            </dl>
          </div>
        ))}
        <div
          className="rounded-lg border border-border p-3"
          data-testid="saturation-panel"
        >
          <p className="text-2xs uppercase tracking-wide text-muted">Saturation</p>
          <p className="mt-1 font-mono text-lg text-foreground">
            {view.saturationPercent !== null ? (
              `${String(view.saturationPercent)}%`
            ) : (
              <span className="text-sm text-muted">unavailable</span>
            )}
          </p>
          <dl className="mt-2 space-y-1 text-2xs text-muted">
            <div>
              <dt className="inline font-medium text-muted-strong">Definition: </dt>
              <dd className="inline">
                Queue depth divided by queue capacity, from one coherent observation.
              </dd>
            </div>
            <div>
              <dt className="inline font-medium text-muted-strong">Denominator: </dt>
              <dd className="inline">queue capacity (work items)</dd>
            </div>
            <div>
              <dt className="inline font-medium text-muted-strong">Supported: </dt>
              <dd className="inline">
                {view.saturationSupported
                  ? "yes — depth and capacity present"
                  : "no — depth or capacity missing from telemetry"}
              </dd>
            </div>
          </dl>
          {view.saturationPercent !== null && (
            <div
              className="mt-2 h-2 w-full rounded bg-surface"
              role="meter"
              aria-valuenow={view.saturationPercent}
              aria-valuemin={0}
              aria-valuemax={100}
              aria-label={`Queue saturation ${String(view.saturationPercent)} percent`}
            >
              <div
                className="h-2 rounded bg-active/70"
                style={{ width: `${Math.min(view.saturationPercent, 100)}%` }}
              />
            </div>
          )}
        </div>
      </div>
      <div className="border-t border-border px-4 py-2">
        {view.health === "disconnected" && (
          <DisconnectedNotice detail="Advisory telemetry only; durable state is unaffected and remains authoritative." />
        )}
        {view.health === "unavailable" && (
          <UnavailableNotice detail="Telemetry is not available for this run (run unknown to telemetry or capacity exhausted)." />
        )}
        {view.stale && (
          <StaleNotice detail="Telemetry has not observed a fresh sample recently; values above are marked stale and no performance conclusion should be drawn from them." />
        )}
        {view.health === "live" && (
          <p className="text-2xs text-muted" role="status">
            Telemetry is advisory and ephemeral. It never advances or replaces durable
            run state.
          </p>
        )}
        {view.health === "idle" && (
          <p className="text-2xs text-muted" role="status">
            No telemetry snapshot has arrived yet; every metric is unavailable until one
            does.
          </p>
        )}
      </div>
    </div>
  );
}
