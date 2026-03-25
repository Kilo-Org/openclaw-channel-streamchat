import { defineConfig } from "vitest/config";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  resolve: {
    alias: [
      {
        find: "openclaw/plugin-sdk/test-utils",
        replacement: path.join(
          __dirname,
          "node_modules",
          "openclaw",
          "dist",
          "plugin-sdk",
          "test-utils.js",
        ),
      },
      {
        find: "openclaw/plugin-sdk",
        replacement: path.join(
          __dirname,
          "node_modules",
          "openclaw",
          "dist",
          "plugin-sdk",
          "index.js",
        ),
      },
    ],
  },
  test: {
    pool: "forks",
    include: ["src/**/*.test.ts", "*.test.ts"],
    unstubEnvs: true,
    unstubGlobals: true,
  },
});
