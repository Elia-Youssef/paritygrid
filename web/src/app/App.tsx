import { Overview } from "../features/overview/overview";
import { ApplicationShell } from "./application-shell";

export function App() {
  return (
    <ApplicationShell>
      <Overview />
    </ApplicationShell>
  );
}
