/**
 * Bundle + minify the MCP server into a single file with the per-build
 * HMAC secret baked in.
 *
 *   INBETWEEN_BUILD_SECRET=... npm run build
 *
 * Without the env var the bundle is built with an empty secret — fine
 * for local smoke tests, NOT for npm publish. CI / the publish step
 * MUST pass the secret.
 *
 * Output: dist/index.js (single file, ESM, minified, with shebang).
 * Dependencies (`@modelcontextprotocol/sdk`, `ws`, `zod`) stay external
 * and npm resolves them at install time.
 */
import { build } from "esbuild";
import { readFileSync, rmSync } from "fs";

const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf-8"));

// Wipe dist so leftover .d.ts / .js from a previous tsc build don't ship.
rmSync(new URL("../dist", import.meta.url), { recursive: true, force: true });
const SECRET = process.env.INBETWEEN_BUILD_SECRET || "";

if (!SECRET) {
  console.warn(
    "⚠ INBETWEEN_BUILD_SECRET is empty. Bundle will be unsigned — fine " +
      "for dev smoke tests, NOT for `npm publish`. Set it in CI or " +
      "before publish.",
  );
}

await build({
  entryPoints: ["src/index.ts"],
  bundle: true,
  platform: "node",
  target: "node18",
  format: "esm",
  outfile: "dist/index.js",
  minify: false,
  sourcemap: false,
  legalComments: "inline",
  banner: { js: "#!/usr/bin/env node" },
  define: {
    "process.env.INBETWEEN_BUILD_SECRET": JSON.stringify(SECRET),
    "process.env.INBETWEEN_CLIENT_VERSION": JSON.stringify(pkg.version),
  },
  external: ["@modelcontextprotocol/sdk", "ws", "zod"],
});

console.log(`✓ built dist/index.js (v${pkg.version})`);
