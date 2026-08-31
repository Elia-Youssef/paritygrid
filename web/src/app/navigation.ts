import {
  Activity,
  GitCompareArrows,
  LayoutDashboard,
  Settings2,
  Workflow,
  type LucideIcon,
} from "lucide-react";

import type { CommandDestination } from "../components/ui/command-palette";

export interface NavigationItem {
  to: string;
  icon: LucideIcon;
  label: string;
  description: string;
  /** Match only the exact location instead of any nested route. */
  end?: boolean;
}

export const primaryNavigation: readonly NavigationItem[] = [
  {
    to: "/app",
    icon: LayoutDashboard,
    label: "Overview",
    description: "Operations overview",
    end: true,
  },
  {
    to: "/app/pipelines",
    icon: Workflow,
    label: "Pipelines",
    description: "Pipeline library and studio",
  },
  {
    to: "/app/runs",
    icon: Activity,
    label: "Runs",
    description: "Run history and live execution",
  },
  {
    to: "/app/compare",
    icon: GitCompareArrows,
    label: "Compare",
    description: "Runner comparison",
  },
];

export const secondaryNavigation: readonly NavigationItem[] = [
  {
    to: "/app/system",
    icon: Settings2,
    label: "System",
    description: "Capabilities and storage health",
  },
];

/** Command-palette destinations: public entry plus every operational area. */
export const commandDestinations: readonly CommandDestination[] = [
  {
    to: "/",
    label: "Public overview",
    description: "The canonical demonstration story",
  },
  ...[...primaryNavigation, ...secondaryNavigation].map(
    ({ description, label, to }) => ({ to, label, description }),
  ),
];
