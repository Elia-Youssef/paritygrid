import {
  Activity,
  DatabaseZap,
  GitCompareArrows,
  History,
  LayoutDashboard,
  Settings2,
  Workflow,
  type LucideIcon,
} from "lucide-react";

export interface NavigationItem {
  href: string;
  icon: LucideIcon;
  label: string;
}

export const primaryNavigation: readonly NavigationItem[] = [
  { href: "#overview", icon: LayoutDashboard, label: "Overview" },
  { href: "#pipelines", icon: Workflow, label: "Pipelines" },
  { href: "#runs", icon: Activity, label: "Active runs" },
  { href: "#history", icon: History, label: "History" },
  { href: "#compare", icon: GitCompareArrows, label: "Compare" },
];

export const secondaryNavigation: readonly NavigationItem[] = [
  { href: "#storage", icon: DatabaseZap, label: "Storage" },
  { href: "#system", icon: Settings2, label: "System" },
];
