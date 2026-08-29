import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    strictPort: true,
    proxy: {
      "/healthz": "http://127.0.0.1:8000",
      "/readyz": "http://127.0.0.1:8000",
    },
  },
  preview: {
    host: "127.0.0.1",
    port: 4173,
    strictPort: true,
  },
  test: {
    environment: "jsdom",
    globals: true,
    include: ["src/**/*.test.{ts,tsx}"],
    setupFiles: ["./src/test/setup.ts"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json-summary", "lcov"],
      reportsDirectory: "./coverage",
      include: ["src/**/*.{ts,tsx}"],
      exclude: ["src/main.tsx", "src/test/**", "src/api/generated/**"],
      thresholds: {
        branches: 75,
        functions: 75,
        lines: 75,
        statements: 75,
        "src/lib/**": {
          branches: 85,
          functions: 85,
          lines: 85,
          statements: 85,
        },
        "src/live/**": {
          branches: 85,
          functions: 85,
          lines: 85,
          statements: 85,
        },
        "src/api/**": {
          branches: 85,
          functions: 85,
          lines: 85,
          statements: 85,
        },
        "src/app/routes-model.ts": {
          branches: 85,
          functions: 85,
          lines: 85,
          statements: 85,
        },
      },
    },
  },
});
