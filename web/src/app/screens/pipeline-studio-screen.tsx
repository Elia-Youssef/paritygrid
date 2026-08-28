import { SquareDashedMousePointer } from "lucide-react";

import { ScreenPlaceholder } from "./screen-placeholder";

export function PipelineStudioScreen() {
  return (
    <ScreenPlaceholder
      icon={SquareDashedMousePointer}
      title="Pipeline studio"
      lede="Typed graph authoring with port validation, plan preview, and keyboard-operable editing for one pipeline version."
      arrival="The studio arrives with the pipeline authoring workflows."
    />
  );
}
