/**
 * Deployment identity shown persistently in the operations shell. The data
 * root may be overridden at build time; the default matches the local-first
 * demonstration layout.
 */

export interface EnvironmentInfo {
  label: string;
  dataRoot: string;
}

const dataRoot = import.meta.env.VITE_PARITYGRID_DATA_ROOT ?? ".paritygrid/";

export const environmentInfo: EnvironmentInfo = {
  label: "Local workspace",
  dataRoot,
};
