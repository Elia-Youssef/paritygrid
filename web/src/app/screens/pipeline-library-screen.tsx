import { Workflow } from "lucide-react";

import { ScreenPlaceholder } from "./screen-placeholder";

export function PipelineLibraryScreen() {
  return (
    <ScreenPlaceholder
      icon={Workflow}
      title="Pipeline library"
      lede="Published, immutable pipeline versions with search, runner compatibility, and resource limits."
      arrival="The library arrives with the pipeline workflows."
    />
  );
}
