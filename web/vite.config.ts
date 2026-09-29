import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `npm run dev` proxies API calls to the feature API running in Docker on :8000.
// In production the built files are served by the feature API itself.
const api = "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5174,
    proxy: Object.fromEntries(
      ["/stats", "/features", "/users", "/contracts", "/dlq", "/health", "/docs", "/openapi.json"].map((p) => [p, api]),
    ),
  },
});
