import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./app/App";
import "./styles.css";

const root = document.querySelector<HTMLDivElement>("#root");

if (root === null) {
  throw new Error("ParityGrid could not find the application root element.");
}

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
