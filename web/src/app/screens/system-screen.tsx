import { SystemCapabilities } from "../../features/system/system-screen";

export function SystemScreen() {
  return (
    <div className="min-h-0 flex-1 overflow-y-auto p-6">
      <h1
        data-page-title
        tabIndex={-1}
        className="text-lg font-semibold text-foreground"
      >
        System health
      </h1>
      <p className="mt-1 max-w-2xl text-sm text-muted">
        Runtime capabilities reported as available, unavailable with a structured
        reason, or unsupported. Failures are never hidden and unsupported is never shown
        as healthy.
      </p>
      <div className="mt-4">
        <SystemCapabilities />
      </div>
    </div>
  );
}
