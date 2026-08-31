import { useParams } from "react-router";

import { StudioScreen } from "../../features/studio/studio-screen";

export function PipelineStudioScreen() {
  const { pipelineId = "" } = useParams();
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="border-b border-border px-4 py-2">
        <h1
          data-page-title
          tabIndex={-1}
          className="text-sm font-semibold text-foreground"
        >
          Pipeline studio
        </h1>
      </div>
      <StudioScreen pipelineId={pipelineId} />
    </div>
  );
}
