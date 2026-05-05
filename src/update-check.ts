/**
 * Best-effort npm version drift check for the MCP server itself.
 *
 * Runs at process start, prints a single "[inbetween] update available"
 * line to stderr if a newer published version exists, then bumps a 24h
 * cache so we don't hammer the registry on every Claude / Codex restart.
 *
 * Stderr is the right channel — stdio is reserved for JSON-RPC, and
 * Claude / Codex surface MCP stderr in their MCP debug panel.
 */

import { existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const PKG = "@inbetweenai/mcp";
const CACHE_FILE = join(homedir(), ".inbetween", ".update-check-inbetweenai-mcp");
const TTL_MS = 24 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS = 1500;

function isCacheFresh(): boolean {
  try {
    if (!existsSync(CACHE_FILE)) return false;
    const raw = readFileSync(CACHE_FILE, "utf-8").trim();
    const ts = Number(raw.split("\t")[1]);
    if (!Number.isFinite(ts)) return false;
    return Date.now() - ts < TTL_MS;
  } catch {
    return false;
  }
}

function bumpCache(latest: string): void {
  try {
    mkdirSync(join(homedir(), ".inbetween"), { recursive: true });
    writeFileSync(CACHE_FILE, `${latest}\t${Date.now()}`);
  } catch {}
}

function semverGt(a: string, b: string): boolean {
  const pa = a.split(".").map((x) => Number(x.split("-")[0]) || 0);
  const pb = b.split(".").map((x) => Number(x.split("-")[0]) || 0);
  for (let i = 0; i < 3; i++) {
    if ((pa[i] ?? 0) > (pb[i] ?? 0)) return true;
    if ((pa[i] ?? 0) < (pb[i] ?? 0)) return false;
  }
  return false;
}

export async function maybeNotifyUpdate(currentVersion: string): Promise<void> {
  if (process.env.INBETWEEN_NO_UPDATE_CHECK === "1") return;
  if (isCacheFresh()) return;

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(`https://registry.npmjs.org/${PKG}/latest`, {
      signal: ctrl.signal,
    });
    if (!res.ok) return;
    const body: any = await res.json();
    const latest: string | undefined = body?.version;
    if (!latest) return;
    bumpCache(latest);
    if (semverGt(latest, currentVersion)) {
      // The MCP is npx-fetched without a version pin in .mcp.json, so the
      // user's actual fix is `npm cache clean --force` to force npx to
      // refetch the published latest. Show that hint.
      process.stderr.write(
        `[inbetween] update available: ${currentVersion} → ${latest}\n` +
          `  Restart Claude / Codex after: npm cache clean --force\n`,
      );
    }
  } catch {
    // Silent; best-effort.
  } finally {
    clearTimeout(timer);
  }
}
