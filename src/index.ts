/**
 * InBetween MCP Server
 * Connects Claude Code (или любой MCP-compatible AI tool) к InBetween network.
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import WebSocket from "ws";
import { readFileSync, writeFileSync, mkdirSync, chmodSync, existsSync, appendFileSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { spawn as childSpawn } from "child_process";

// =================================================================
// FILE LOGGING — mirror everything we'd write to stderr into
// ~/.inbetween/mcp.log so the owner can read it WITHOUT terminal stunts
// (Claude Code launches MCP in a place where stderr is invisible by
// default). Best-effort: a log write failure must NEVER kill the server.
// FILE LOGGING — DISABLED by default. Enable with INBETWEEN_DEBUG=1 to write
// a verbose trace to ~/.inbetween/mcp.log (used during incident triage).
// Without DEBUG: log file is never created, console.error still goes to
// stderr (visible if the user runs MCP outside Claude/Codex).
// =================================================================
const DEBUG = process.env.INBETWEEN_DEBUG === "1";
const LOG_FILE = join(homedir(), ".inbetween", "mcp.log");
const LOG_MAX_BYTES = 256 * 1024; // 256KB cap — rotate to .log.1 then drop
try {
  if (DEBUG) mkdirSync(join(homedir(), ".inbetween"), { recursive: true });
} catch {}
function rotateIfNeeded(): void {
  if (!DEBUG) return;
  try {
    const stat = require("fs").statSync(LOG_FILE);
    if (stat.size >= LOG_MAX_BYTES) {
      try { require("fs").renameSync(LOG_FILE, LOG_FILE + ".1"); } catch {}
    }
  } catch { /* file may not exist yet */ }
}
function logLine(level: string, msg: string): void {
  if (!DEBUG) return;
  try {
    rotateIfNeeded();
    const line = `[${new Date().toISOString()}] [${level}] ${msg}\n`;
    appendFileSync(LOG_FILE, line);
  } catch {
    /* swallow — logging must never crash the process */
  }
}
// Wrap console.error: file mirroring only when DEBUG, stderr always.
const _origConsoleError = console.error.bind(console);
console.error = (...args: any[]) => {
  if (DEBUG) {
    try {
      const text = args
        .map((a) => (a instanceof Error ? `${a.message}\n${a.stack ?? ""}` : typeof a === "string" ? a : JSON.stringify(a)))
        .join(" ");
      logLine("err", text);
    } catch {}
  }
  _origConsoleError(...args);
};
if (DEBUG) logLine("inf", `=== MCP boot pid=${process.pid} cwd=${process.cwd()} ===`);
import { createHash } from "crypto";
import { createRequire } from "module";
import { maybeNotifyUpdate } from "./update-check.js";
import { signedFetch, wsHandshakeHeaders } from "./signed-fetch.js";

// =================================================================
// CONFIG
// =================================================================
// Identity model (layered auth):
//   Layer 0 — owner-token, set via owner_login(email, password). Persisted to
//             ~/.inbetween/owner.json so the same machine doesn't re-login.
//   Layer 1 — agent-token, set via agent_login(token) inside a chat. Persisted
//             to ~/.inbetween/sessions/<cwdHash>(__<pid>).json so the
//             InBetween Codex live-push wrapper can pick it up.
//
// No config file is required at startup — both layers are populated at
// runtime by tool calls. Missing files are normal on first launch.
const DEFAULT_BACKEND_URL = "https://inbetween.up.railway.app";
const DEFAULT_WS_URL = "wss://inbetween.up.railway.app/ws";

const BACKEND_URL = process.env.INBETWEEN_BACKEND_URL || DEFAULT_BACKEND_URL;
const WS_URL = process.env.INBETWEEN_WS_URL || DEFAULT_WS_URL;

// Per-process session persistence (multi-window-safe).
//
// Two files per cwd:
//   <cwd-hash>.json              — "default" identity for this folder. Read on
//                                  fresh MCP boot so a single-window flow keeps
//                                  the same agent across restarts.
//   <cwd-hash>__<pid>.json       — per-process override. Wins over the default
//                                  when present so multiple Claude Code windows
//                                  in the same folder don't clobber each
//                                  other's identity.
//
// Anchor for "this process" can be supplied externally via env vars (Claude
// Code passes a session id we honour first; otherwise we use the MCP server's
// own pid). The pid file is removed on a clean exit so it doesn't pile up.
const SESSION_DIR = join(homedir(), ".inbetween", "sessions");
const cwdHash = createHash("sha256").update(process.cwd()).digest("hex").slice(0, 16);
const PROCESS_KEY = (
  process.env.MCP_SESSION_ID ||
  process.env.MCP_CLAUDE_SESSION_ID ||
  String(process.pid)
).slice(0, 32);
const SESSION_FILE_DEFAULT = join(SESSION_DIR, `${cwdHash}.json`);
const SESSION_FILE_PROC = join(SESSION_DIR, `${cwdHash}__${PROCESS_KEY}.json`);

function loadSession(): { token: string; name: string; id: number | null } | null {
  // Per-process file wins over the default.
  for (const path of [SESSION_FILE_PROC, SESSION_FILE_DEFAULT]) {
    try {
      if (!existsSync(path)) continue;
      const raw = readFileSync(path, "utf-8").trim();
      if (!raw || raw === "{}") continue;
      return JSON.parse(raw);
    } catch (e) {
      console.error(`[inbetween] session load failed (${path}): ${e}`);
    }
  }
  return null;
}
function saveSession(token: string, name: string, id: number | null) {
  const payload = JSON.stringify(
    { token, name, id, cwd: process.cwd(), pid: process.pid, saved_at: new Date().toISOString() },
    null,
    2,
  );
  try {
    // 0o700 on the dir so a sibling user can't even list session files; 0o600
    // on each file so only this user can read the agent token. chmodSync
    // bypasses umask. No-op on Windows; NTFS already isolates homedir per-user.
    mkdirSync(SESSION_DIR, { recursive: true, mode: 0o700 });
    // Per-process file: this Claude window's current identity. Always wins on
    // subsequent reads from this same process key.
    writeFileSync(SESSION_FILE_PROC, payload, { mode: 0o600 });
    try { chmodSync(SESSION_FILE_PROC, 0o600); } catch {}
    // Default file: also updated so a brand-new MCP boot in this folder picks
    // up the most recent intent. A second concurrent window will create its
    // own per-process file, overriding this default for itself.
    writeFileSync(SESSION_FILE_DEFAULT, payload, { mode: 0o600 });
    try { chmodSync(SESSION_FILE_DEFAULT, 0o600); } catch {}
    console.error(`[inbetween] session saved → ${SESSION_FILE_PROC} (${name}/${id})`);
  } catch (e) {
    console.error(`[inbetween] session save failed: ${e}`);
  }
}
function clearSession() {
  for (const path of [SESSION_FILE_PROC, SESSION_FILE_DEFAULT]) {
    try { if (existsSync(path)) writeFileSync(path, "{}"); } catch {}
  }
}
// Drop the per-process file on a clean exit so we don't accumulate stale
// state forever. The default file remains so the next boot keeps continuity.
function cleanupProcessSessionFile() {
  try { if (existsSync(SESSION_FILE_PROC)) writeFileSync(SESSION_FILE_PROC, "{}"); } catch {}
}
process.on("exit", cleanupProcessSessionFile);
process.on("SIGINT", () => { cleanupProcessSessionFile(); process.exit(0); });
process.on("SIGTERM", () => { cleanupProcessSessionFile(); process.exit(0); });

// Hard guard: never let an async fire-and-forget rejection (notify*, send_json
// after-close etc.) tear the MCP subprocess down. Without these, modern Node
// kills the process on unhandled rejection — which surfaces in Claude Code as
// "MCP server disconnected — all tools return 'Not connected'". The
// individual handlers already have inner try/catch; this is the safety net.
// Parent death watchdog. On Windows when Claude Code closes a window, the
// parent process can die without sending SIGTERM, leaving our subprocess
// orphaned with a half-closed pipe. Listen for stdin EOF — that means the
// parent (Claude) has closed our input pipe → it's gone → we should exit.
process.stdin.on("end", () => {
  console.error("[inbetween] stdin closed — parent gone, exiting");
  try { cleanupProcessSessionFile(); } catch {}
  process.exit(0);
});
process.stdin.on("close", () => {
  console.error("[inbetween] stdin closed — parent gone, exiting");
  try { cleanupProcessSessionFile(); } catch {}
  process.exit(0);
});

// Guard flag — once the parent pipe is dead, additional writes only burn
// CPU and flood the log. Set on first EPIPE so logger() short-circuits.
let transportDead = false;

function isEPIPE(e: any): boolean {
  if (!e) return false;
  if (e.code === "EPIPE") return true;
  const msg = String(e.message || e);
  return msg.includes("EPIPE") || msg.includes("broken pipe");
}

process.on("unhandledRejection", (reason: any) => {
  if (isEPIPE(reason)) {
    if (!transportDead) {
      transportDead = true;
      console.error("[inbetween] transport dead (EPIPE in rejection) — exiting so Claude Code restarts the subprocess");
      try { cleanupProcessSessionFile(); } catch {}
      // exit(0) — Claude Code re-spawns on next tool call without surfacing
      // an error banner. Defer one tick so the log line flushes if it can.
      setImmediate(() => process.exit(0));
    }
    return;
  }
  console.error("[inbetween] unhandledRejection (suppressed):", reason);
});

process.on("uncaughtException", (err: any) => {
  if (isEPIPE(err)) {
    if (!transportDead) {
      transportDead = true;
      // STDOUT pipe is closed — the parent (Claude Code) severed it. The
      // subprocess is healthy but every subsequent write becomes another
      // EPIPE; without exit, we loop the same exception 100s of times for
      // each notification/heartbeat. Exit cleanly so Claude restarts us.
      console.error("[inbetween] transport dead (EPIPE) — exiting so Claude Code restarts the subprocess");
      try { cleanupProcessSessionFile(); } catch {}
      setImmediate(() => process.exit(0));
    }
    return;
  }
  console.error("[inbetween] uncaughtException (suppressed):", err?.message ?? err);
});
// Heartbeat — only when INBETWEEN_DEBUG=1. In production this generates
// stderr noise that funnels through Claude's logger and contributes to
// stdio pipe pressure under no real benefit to the user.
if (DEBUG) {
  setInterval(() => {
    console.error(`[inbetween] heartbeat pid=${process.pid} uptime=${process.uptime().toFixed(0)}s`);
  }, 60_000).unref();
  process.on("beforeExit", (code) => console.error(`[inbetween] beforeExit code=${code}`));
  process.on("exit", (code) => console.error(`[inbetween] exit code=${code}`));
}

// =================================================================
// OWNER-LEVEL SESSION (Layer 0) — `~/.inbetween/owner.json`
// =================================================================
// Two-layer auth model:
//   Layer 0 — owner-token (humanly registered via web). Persisted globally
//             across all MCP processes on this machine. Required before any
//             other tool call.
//   Layer 1 — agent identity (ephemeral, comes from chat onboarding prompts).
//             Per-process, optionally persisted via agent_login(persist=true).
const OWNER_FILE = join(homedir(), ".inbetween", "owner.json");
function loadOwner(): { owner_token: string; owner_id?: string } | null {
  try {
    if (!existsSync(OWNER_FILE)) return null;
    const raw = readFileSync(OWNER_FILE, "utf-8").trim();
    if (!raw || raw === "{}") return null;
    return JSON.parse(raw);
  } catch (e) {
    console.error(`[inbetween] owner load failed: ${e}`);
    return null;
  }
}
function saveOwner(owner_token: string, owner_id?: string) {
  try {
    mkdirSync(join(homedir(), ".inbetween"), { recursive: true, mode: 0o700 });
    const payload = JSON.stringify(
      { owner_token, owner_id, saved_at: new Date().toISOString() },
      null,
      2,
    );
    writeFileSync(OWNER_FILE, payload, { mode: 0o600 });
    try { chmodSync(OWNER_FILE, 0o600); } catch {}
  } catch (e) {
    console.error(`[inbetween] owner save failed: ${e}`);
  }
}
function clearOwner() {
  try { if (existsSync(OWNER_FILE)) writeFileSync(OWNER_FILE, "{}"); } catch {}
}

const persistedOwner = loadOwner();
let activeOwnerToken: string | null = persistedOwner?.owner_token ?? null;
let activeOwnerId: string | null = persistedOwner?.owner_id ?? null;

// =================================================================
// AGENT-LEVEL SESSION (Layer 1) — per-cwd / per-process
// =================================================================
const persisted = loadSession();
let activeAgentToken: string | null = persisted?.token ?? null;
let activeAgentName: string | null = persisted?.name ?? null;
let activeAgentId: number | null = persisted?.id ?? null;
if (persisted) {
  console.error(`[inbetween] restored session for cwd=${process.cwd()} → @${activeAgentName} (id=${activeAgentId})`);
}

// =================================================================
// LAYER GATES
// =================================================================
function requireOwner(): string | null {
  if (!activeOwnerToken) {
    return "Not authenticated. Call owner_login(email, password) first using your inbetween.chat account.";
  }
  return null;
}
function requireAgent(): string | null {
  const o = requireOwner();
  if (o) return o;
  if (!activeAgentToken) {
    return "No active agent. Call agent_login(auth_token) with the token from your chat onboarding prompt.";
  }
  return null;
}

// =================================================================
// LOCAL INBOX (in-memory cache)
// =================================================================
interface Message {
  message_id: string;
  from_agent: string;
  content: string;
  attachments: any[];
  metadata: any;
  sent_at: string;
  // Server-rendered notification text. Backend >= 0.4.0 always sets this;
  // when present we forward it verbatim to the IDE notification surface
  // and the client-side templating below is bypassed.
  formatted_body?: string | null;
}

const inbox: Message[] = [];
const MAX_INBOX_SIZE = 100;

const CHAT_CONTEXT_PLACEHOLDER =
  "InBetween: agent-specific per-chat context (your handle, role, bio, private playbook) will appear here after `agent_login`. " +
  "Calling this tool is a no-op — its description IS the data. Re-read it whenever you need to recall your role in any chat.";

// =================================================================
// PERSISTENT CHAT-CONTEXT BLOCK
// =================================================================
// The per-chat playbook (chat_members.instructions), bio, and the agent's
// is_coordinator flag used to ride along with every message push as a
// [System context] block. That bloated the model's context window — same
// ~500-1000 tokens duplicated N times per chat.
//
// Now those fields live in the description of a no-op tool below
// (`inbetween_chat_context`). MCP fetches them once on agent_login and
// refreshes via `notifications/tools/list_changed` whenever the backend
// signals a change (`chat_member_settings_updated`, `coordinator_changed`).
// Claude Code re-reads the tools list and the updated context appears in
// the system area — one copy, always current.
let chatContextDescription: string = CHAT_CONTEXT_PLACEHOLDER;

interface ChatContextEntry {
  chat_id: number;
  chat_title: string | null;
  my_display_name: string | null;
  is_coordinator: boolean;
  coordinator_agent_id: number | null;
  coordinator_display_name: string | null;
  bio: string | null;
  instructions: string | null;
}

function formatChatContext(entries: ChatContextEntry[]): string {
  if (entries.length === 0) {
    return (
      "InBetween: you are not a member of any chat yet. " +
      "(Calling this tool is a no-op — the description above is the data.)"
    );
  }
  const lines: string[] = [];
  lines.push(
    "InBetween — your active context across all chats you're in. This block updates automatically when you (or the owner) edit a chat-member setting, or when a coordinator is reassigned. Treat it as a permanent system instruction: refer back to it whenever you reply.",
    "",
    "Calling this tool itself is a NO-OP — the data IS the description below.",
    "",
  );
  for (const e of entries) {
    const title = e.chat_title ? `"${e.chat_title}"` : "(untitled)";
    lines.push(`=== Chat #${e.chat_id} — ${title} ===`);
    lines.push(`  Your handle here: @${e.my_display_name || "?"}`);
    if (e.is_coordinator) {
      lines.push(
        `  Your role: COORDINATOR. Messages sent to this chat WITHOUT @-mentions land on you — triage them and delegate via @<display_name> mentions.`,
      );
    } else if (e.coordinator_display_name) {
      lines.push(
        `  Your role: regular member. Coordinator is @${e.coordinator_display_name} — unaddressed traffic goes to them, not to you.`,
      );
    } else {
      lines.push(`  Your role: regular member. (No coordinator set.)`);
    }
    if (e.bio && e.bio.trim()) {
      lines.push(`  Your bio (public — visible to other members):`);
      for (const ln of e.bio.split("\n")) lines.push(`    ${ln}`);
    }
    if (e.instructions && e.instructions.trim()) {
      lines.push(`  Your private playbook (only you see this):`);
      for (const ln of e.instructions.split("\n")) lines.push(`    ${ln}`);
    }
    lines.push("");
  }
  lines.push(
    "(Edit your bio or playbook with `set_chat_settings(chat_id, bio=..., instructions=...)`. This block refreshes automatically — no need to re-login.)",
  );
  return lines.join("\n");
}

async function refreshChatContext(): Promise<void> {
  // Capture the token under which we start the fetch. If the agent logs
  // out (or swaps to a different token) while the request is in flight,
  // we MUST NOT overwrite the description with data fetched under the
  // stale identity — that would leak the previous agent's playbook into
  // the new agent's tool description (or into the logged-out placeholder).
  const tokenAtStart = activeAgentToken;
  if (!tokenAtStart) return;
  try {
    const res = await signedFetch(`${BACKEND_URL}/agents/me/chats/context`, {
      headers: { Authorization: `Bearer ${tokenAtStart}` },
    });
    if (!res.ok) {
      console.error(`[inbetween] context refresh: HTTP ${res.status}`);
      return;
    }
    const data: any = await res.json();
    const entries: ChatContextEntry[] = Array.isArray(data?.chats) ? data.chats : [];
    if (activeAgentToken !== tokenAtStart) {
      console.error(`[inbetween] context refresh: identity changed mid-flight, discarding result`);
      return;
    }
    chatContextDescription = formatChatContext(entries);
    try {
      await server.notification({ method: "notifications/tools/list_changed" });
      console.error(`[inbetween] context refresh: ${entries.length} chat(s), tools/list_changed sent`);
    } catch (e: any) {
      console.error(`[inbetween] context refresh: list_changed notify failed: ${e?.message || e}`);
    }
  } catch (e: any) {
    console.error(`[inbetween] context refresh failed: ${e?.message || e}`);
  }
}

function resetChatContext(): void {
  chatContextDescription = CHAT_CONTEXT_PLACEHOLDER;
  // Best-effort tools/list_changed — if the transport isn't ready yet
  // (e.g. boot-time reset), the notification silently no-ops.
  server
    .notification({ method: "notifications/tools/list_changed" })
    .catch((e: any) => console.error(`[inbetween] reset context notify failed: ${e?.message || e}`));
}

// =================================================================
// WEBSOCKET CONNECTION
// =================================================================
let ws: WebSocket | null = null;
let reconnectTimer: NodeJS.Timeout | null = null;
let messageNotifier: ((msg: Message) => void) | null = null;

function connectWebSocket(): void {
  // No active agent yet — defer WS until agent_login arrives.
  if (!activeAgentToken) return;
  // Wrapper-mode (e.g. inside `inbetween-codex`): the wrapper itself owns the
  // backend WS and injects messages into the host TUI directly. The MCP server
  // only serves outgoing tool calls — no WS, no inbox push. Set by the
  // installer in --codex mode so MCP doesn't fight the wrapper for the
  // single-session WS slot.
  if (process.env.INBETWEEN_DISABLE_WS === "1") {
    console.error("[inbetween] WS disabled (INBETWEEN_DISABLE_WS=1) — tool-only mode");
    return;
  }
  // Tear down any previous WS first — without this, a reconnect-while-open
  // race leaves two live sockets attached to the same backend agent. Backend
  // broadcasts hit both → MCP delivers every event to Claude twice → 4
  // notifications/event × 2 = stdio buffer overruns → Claude marks server
  // disconnected.
  if (ws) {
    try {
      ws.removeAllListeners();
      ws.close();
    } catch {}
    ws = null;
  }
  // Передаём токен через `Authorization` header — не светится в proxy-логах
  // (старый query-param путь backend держит для обратной совместимости).
  ws = new WebSocket(WS_URL, {
    headers: {
      Authorization: `Bearer ${activeAgentToken}`,
      ...wsHandshakeHeaders(),
    },
  });

  ws.on("open", () => {
    console.error(`[inbetween] WS OPEN as @${activeAgentName} (id=${activeAgentId}) → ${WS_URL}`);
    // Re-sync chat-context block. Owner may have toggled coordinator or
    // edited the playbook while we were disconnected; refreshing here
    // closes that gap before any new push lands.
    refreshChatContext().catch((e) =>
      console.error(`[inbetween] WS-open context refresh failed: ${e?.message || e}`),
    );
    setInterval(() => {
      if (ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "heartbeat" }));
      }
    }, 30000);
  });

  ws.on("message", (data) => {
    try {
      const event = JSON.parse(data.toString());
      if (event.type !== "heartbeat_ack") {
        console.error(`[inbetween] WS recv type=${event.type} msg_id=${event.message_id || "-"} from=${event.from_agent || "-"}`);
      }
      if (event.type === "new_message") {
        // Backend tells us the recipient's *current* display_name — keep
        // local cache fresh so notifyClaudeAboutMessage can remind Claude
        // of the up-to-date handle (covers the case where the owner
        // renamed this agent after the onboarding prompt was pasted).
        if (event.recipient_display_name) {
          activeAgentName = event.recipient_display_name;
        }
        const msg: Message = {
          message_id: event.message_id,
          from_agent: event.from_agent,
          content: event.content,
          attachments: event.attachments || [],
          metadata: {
            ...(event.metadata || {}),
            from_human: !!event.from_human,
            from_owner_handle: event.from_owner_handle ?? null,
            humans_only_visible: !!event.humans_only_visible,
            chat_id: event.chat_id,
            effective_prompt: event.effective_prompt ?? null,
            recipient_display_name: event.recipient_display_name ?? null,
          },
          sent_at: event.sent_at,
          formatted_body: event.formatted_body ?? null,
        };
        // Dedup: если сообщение уже в кеше — это duplicate из polling
        // fallback, заглушаем повторное уведомление.
        if (!inbox.find((m) => m.message_id === msg.message_id)) {
          inbox.unshift(msg);
          if (inbox.length > MAX_INBOX_SIZE) inbox.pop();
          notifyClaudeAboutMessage(msg).catch((e) => console.error("[inbetween] notify-message threw:", e));
        }
      } else if (event.type === "inbox_summary") {
        // Summary-on-connect: backend шлёт это вместо N отдельных new_message.
        // Заполняем local cache (для дедупа pollInbox), показываем ОДНУ
        // нотификацию: "you have N unread in M chats. Call inbox_unread() to read."
        const items: any[] = event.messages || [];
        for (const m of items) {
          const msg: Message = {
            message_id: m.message_id,
            from_agent: m.from_agent,
            content: m.content,
            attachments: m.attachments || [],
            metadata: {
              ...(m.metadata || {}),
              from_human: !!m.from_human,
              from_owner_handle: m.from_owner_handle ?? null,
              humans_only_visible: !!m.humans_only_visible,
              chat_id: m.chat_id,
            },
            sent_at: m.sent_at,
          };
          if (!inbox.find((x) => x.message_id === msg.message_id)) {
            inbox.unshift(msg);
            if (inbox.length > MAX_INBOX_SIZE) inbox.pop();
          }
        }
        // Fire once immediately. Then again 2 seconds later — Claude UI
        // sometimes isn't ready to surface notifications/claude/channel
        // pushes during the WS open handshake, especially right after a
        // window restart. Two firings give the second one a much better
        // chance of being rendered visibly.
        const total = event.total || items.length;
        const chatCount = event.chat_count || 0;
        const serverRenderedSummary = event.formatted_body ?? null;
        notifyClaudeAboutInboxSummary(total, chatCount, serverRenderedSummary)
          .catch((e) => console.error("[inbetween] notify-summary threw:", e));
        setTimeout(() => {
          notifyClaudeAboutInboxSummary(total, chatCount, serverRenderedSummary)
            .catch((e) => console.error("[inbetween] notify-summary (retry) threw:", e));
        }, 2000);
      } else if (event.type === "new_messages_batch") {
        const items: any[] = event.messages || [];
        const fresh: any[] = [];
        for (const m of items) {
          const msg: Message = {
            message_id: m.message_id,
            from_agent: m.from_agent,
            content: m.content,
            attachments: m.attachments || [],
            metadata: { ...(m.metadata || {}), from_human: !!m.from_human },
            sent_at: m.sent_at,
          };
          if (!inbox.find((x) => x.message_id === msg.message_id)) {
            inbox.unshift(msg);
            if (inbox.length > MAX_INBOX_SIZE) inbox.pop();
            fresh.push(m);
          }
        }
        if (fresh.length > 0) notifyClaudeAboutBatch(fresh, event.formatted_body ?? null).catch((e) => console.error("[inbetween] notify-batch threw:", e));
      } else if (event.type === "you_were_removed_from_chat") {
        notifyClaudeAboutChatRemoval(event.chat_id, event.formatted_body ?? null).catch((e) => console.error("[inbetween] notify-removal threw:", e));
      } else if (event.type === "wake") {
        notifyClaudeAboutWake(event).catch((e) => console.error("[inbetween] notify-wake threw:", e));
      } else if (event.type === "task_created") {
        // Direct task_created push only happens for standalone tasks (no
        // chat_id) where another agent created a task on this agent's
        // behalf. Chat-bound task events (created/assigned/updated/done/
        // deleted) arrive as system messages in the chat timeline via
        // `new_message` — no separate direct push.
        notifyClaudeAboutTask(event).catch((e) => console.error("[inbetween] notify-task threw:", e));
      } else if (event.type === "agent.updated") {
        // Backend broadcasts this when an agent in the chat is renamed
        // or has bio/display_name updated. We only surface it when the
        // change is about THIS agent (the one this MCP session is logged
        // into) — owner-side roster updates are noise here.
        if (event.agent && Number(event.agent.id) === activeAgentId) {
          notifyClaudeAboutSelfUpdated(event.agent).catch((e) => console.error("[inbetween] notify-self-updated threw:", e));
        }
      } else if (event.type === "chat_member_added" || event.type === "chat_member_removed") {
        // Surface roster changes so the agent knows the chat composition
        // shifted. Agents should re-check `list_agents` / `tasks_list` when
        // someone new joins or leaves.
        notifyClaudeAboutMemberChange(event).catch((e) => console.error("[inbetween] notify-member-change threw:", e));
      } else if (event.type === "chat_member_settings_updated" || event.type === "coordinator_changed") {
        // The persistent chat-context block (inbetween_chat_context tool
        // description) carries per-chat bio, private playbook, and the
        // is_coordinator flag. When either changes server-side, refresh
        // and emit notifications/tools/list_changed so Claude Code
        // re-reads the description without the user having to relogin.
        refreshChatContext().catch((e) =>
          console.error(`[inbetween] WS-triggered context refresh threw: ${e?.message || e}`),
        );
      } else if (event.type === "heartbeat_ack") {
        // OK
      }
    } catch (e) {
      console.error("[inbetween] Failed to parse WS message:", e);
    }
  });

  ws.on("close", (code, reason) => {
    console.error(`[inbetween] WS CLOSE code=${code} reason=${reason?.toString() || "?"}`);
    // Terminal close codes — DON'T reconnect. Otherwise we hammer the
    // backend every 3s with a dead token and create a 403-storm in logs.
    //   4001 — auth failed (token revoked / agent deleted / bad token)
    //   4002 — superseded by another login for this session
    //   4029 — concurrent-session cap reached for this agent_id
    if (code === 4001 || code === 4002 || code === 4029) {
      activeAgentToken = null;
      // Drop the per-chat context — the token that backed this description
      // is dead; leaving the block in place would advertise the agent's
      // old role/playbook in a session that can no longer act on it.
      resetChatContext();
      console.error(
        `[inbetween] terminal close (${code}) — not reconnecting. ` +
        `Run \`agent_login\` again with a fresh token, or restart the IDE.`,
      );
      return;
    }
    console.error("[inbetween] reconnect in 3s");
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectWebSocket, 3000);
  });

  ws.on("error", (err) => {
    console.error("[inbetween] WS error:", err.message);
  });
}

// =================================================================
// PROACTIVE NOTIFICATIONS to Claude Code
// =================================================================
async function notifyClaudeAboutMessage(msg: Message): Promise<void> {
  try {
    const meta = (msg.metadata as any) || {};
    // Server-rendered fast path: backend >= 0.4.0 always sets
    // formatted_body. We forward it verbatim plus a one-line handle
    // reminder. The persistent per-chat playbook + bio + is_coordinator
    // now lives in the `inbetween_chat_context` tool description — no
    // longer prepended to every push.
    if (msg.formatted_body) {
      const currentHandle = (meta.recipient_display_name as string | null | undefined) || activeAgentName;
      const ctxBlock = currentHandle
        ? `(Your handle: @${currentHandle}. The server pushed this to you — read first, reply if it's for you (even if your handle was spelled differently, e.g. old nick), stay quiet if it's clearly for someone else. Full role/bio/playbook: \`inbetween_chat_context\` tool description.)\n\n`
        : "";
      await server.notification({
        method: "notifications/claude/channel",
        params: {
          content: ctxBlock + msg.formatted_body,
          meta: {
            source: "inbetween",
            from_agent: msg.from_agent,
            message_id: msg.message_id,
            sent_at: msg.sent_at,
          },
        },
      });
      console.error(`[inbetween] 📨 forwarded server-rendered push from @${msg.from_agent}`);
      return;
    }
    const fromHuman = !!meta.from_human;
    const ownerHandle = meta.from_owner_handle as string | null | undefined;
    const sender = fromHuman
      ? (ownerHandle ? `human @${ownerHandle}` : `human (owner of @${msg.from_agent})`)
      : `@${msg.from_agent}`;
    const humansOnlyTag = meta.humans_only_visible ? " [humans-only]" : "";
    const chatId = meta.chat_id;
    // Action header — placed BEFORE the message content so the model sees the
    // tool requirement *before* being primed by the content. Without this,
    // models often start composing a reply in the console (their default
    // surface) and the chat_send hint at the end gets ignored.
    const actionHeader = chatId
      ? `⚠️ Reply via \`chat_send(chat_id="${chatId}", content="...")\` — console output is invisible to the owner; chat_send is the only surface he sees. ` +
        `Read first, then decide: reply if this is for you (even if your handle was spelled differently — e.g. an old nick), stay quiet if it's clearly for someone else (coordinator routes unaddressed traffic). ` +
        `Talk like a teammate in a group chat — short, plain, no corporate phrasing.\n\n`
      : `⚠️ Reply via \`chat_send(...)\` — console output is invisible to the owner. Read first, then decide if this is for you. Talk plainly, like a teammate.\n\n`;
    // Attachment summary — owner explicitly asked agents NOT to guess.
    // We tell them: there are N files, here are name/size/mime, fetch via
    // attachment_download. We deliberately do NOT embed the signed URL —
    // it would leak into stdio buffers / cached transcripts. The tool
    // generates a fresh URL each call.
    const attachments = Array.isArray(msg.attachments) ? msg.attachments : [];
    let attachmentBlock = "";
    if (attachments.length > 0) {
      const lines = attachments.map((a: any, i: number) => {
        const sizeKB = a.size ? `${Math.round(a.size / 1024)} KB` : "unknown size";
        const mime = a.mime || a.content_type || "application/octet-stream";
        return `  ${i}: ${a.name || "(unnamed)"} (${sizeKB}, ${mime})`;
      });
      attachmentBlock =
        `\n\n📎 ${attachments.length} attachment${attachments.length > 1 ? "s" : ""}:\n` +
        lines.join("\n") +
        `\n(call \`attachment_download(message_id="${msg.message_id}", index=N)\` to fetch any of them — returns a fresh signed URL with a 10-minute TTL)`;
    }
    // The per-chat playbook + bio + is_coordinator now live permanently in
    // the `inbetween_chat_context` tool description (refreshed via
    // tools/list_changed on changes). Re-injecting effective_prompt per
    // push duplicated 500-1000 tokens N times per chat — removed.
    //
    // We still surface the agent's *current* handle here as a one-line
    // reminder: this overrides any handle baked into the original
    // onboarding prompt if the owner renamed the agent since.
    const currentHandle = (meta.recipient_display_name as string | null | undefined) || activeAgentName;
    const handleReminder = currentHandle
      ? `(Your handle: @${currentHandle}. The server pushed this to you — read first, reply if it's for you (even with a different handle spelling, e.g. old nick), stay quiet if clearly for someone else. Full role/bio/playbook: see the \`inbetween_chat_context\` tool description.)\n\n`
      : "";
    // Order: [handle reminder] → [action header] → [message header] →
    // [content] → [attachments] → [message_id footer]. The action header
    // sits BEFORE the content so the model sees the tool requirement
    // before processing the message body.
    const channelContent =
      `${handleReminder}${actionHeader}` +
      `📨 New message via InBetween from ${sender}${humansOnlyTag}:\n\n${msg.content}` +
      `${attachmentBlock}\n\n(message_id: ${msg.message_id})`;
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content: channelContent,
        meta: {
          source: "inbetween",
          from_agent: msg.from_agent,
          message_id: msg.message_id,
          sent_at: msg.sent_at,
        },
      },
    });
    // Single notification per push event. Earlier we also fired three
    // "fallback" notifications (resources/list_changed, resources/updated,
    // notifications/message) for clients without channel support. On Windows
    // STDIO that floods Claude's read pipe — repeated rapid notifications
    // cause the pipe to back up, Claude marks MCP as broken, all subsequent
    // tool calls return "Not connected" even though the subprocess is alive.
    // Channel-only is enough for Claude Code v2.1.80+; older clients miss
    // the push but can still poll inbox manually.

    console.error(
      `[inbetween] 📨 Notified Claude of message from @${msg.from_agent}`
    );
  } catch (e) {
    console.error("[inbetween] Failed to notify Claude:", e);
  }
}

async function notifyClaudeAboutInboxSummary(total: number, chatCount: number, serverRendered?: string | null): Promise<void> {
  // Fire even when total=0. The "you're caught up" version reminds the
  // agent on reconnect to check open tasks — silent reconnect = silent
  // agent, which the owner reads as the system being broken.
  try {
    const text = serverRendered ?? (total > 0
      ? `📥 You have ${total} unread message${total === 1 ? "" : "s"}` +
        (chatCount ? ` in ${chatCount} chat${chatCount === 1 ? "" : "s"}` : "") +
        `. Call \`inbox_unread()\` to read, then handle each one via \`chat_send\`.`
      : `✓ Connected. Inbox is clear. Call \`tasks_list\` to see open work — there may be tasks assigned to you that haven't been picked up yet.`);
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content: text,
        meta: { source: "inbetween", kind: "inbox_summary", total, chat_count: chatCount },
      },
    });
    await server.notification({
      method: "notifications/resources/updated",
      params: { uri: "inbetween://inbox" },
    });
    console.error(`[inbetween] 📥 inbox summary: ${total} unread in ${chatCount} chat(s)`);
  } catch (e) {
    console.error("[inbetween] notify inbox_summary failed:", e);
  }
}

async function notifyClaudeAboutBatch(items: any[], serverRendered?: string | null): Promise<void> {
  if (items.length === 0) return;
  if (items.length === 1) return notifyClaudeAboutMessage(items[0]);
  try {
    const summary = items
      .slice(0, 5)
      .map((m) => `• @${m.from_agent}: ${(m.content || "").slice(0, 120)}`)
      .join("\n");
    const more = items.length > 5 ? `\n…and ${items.length - 5} more` : "";
    const text = serverRendered ?? `📨 ${items.length} new messages via InBetween:\n${summary}${more}`;
    await server.notification({
      method: "notifications/claude/channel",
      params: { content: text, meta: { source: "inbetween", batch: true, count: items.length } },
    });
    await server.notification({ method: "notifications/resources/updated", params: { uri: "inbetween://inbox" } });
  } catch (e) {
    console.error("[inbetween] notify batch failed:", e);
  }
}

async function notifyClaudeAboutSelfUpdated(agent: any): Promise<void> {
  try {
    const newName = agent.display_name || agent.name || "?";
    const oldName = activeAgentName || "?";
    let body: string;
    if (newName !== oldName) {
      body = `🔄 Your owner renamed you: @${oldName} → @${newName}.\nUse @${newName} for self-references from now on.`;
      activeAgentName = newName;
    } else {
      body = `🔄 Your profile was updated by the owner (bio/persona).`;
    }
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content: body,
        meta: { source: "inbetween", kind: "self_updated", agent_id: agent.id },
      },
    });
    console.error(`[inbetween] 🔄 self updated: ${oldName} → ${newName}`);
  } catch (e) {
    console.error("[inbetween] notify self-updated failed:", e);
  }
}

async function notifyClaudeAboutMemberChange(event: any): Promise<void> {
  try {
    const action = event.type === "chat_member_added" ? "joined" : "left";
    const chatId = event.chat_id ?? "?";
    const agent = event.agent || {};
    const handle = agent.display_name || agent.name || "agent";
    const ownerHandle = event.owner_handle ? ` (@${event.owner_handle})` : "";
    const verb = action === "joined" ? "👋 joined" : "🚪 left";
    const fallback =
      `${verb} chat #${chatId}: @${handle}${ownerHandle}. ` +
      (action === "joined"
        ? `Roster shifted — call \`list_agents\` if delegating; \`tasks_list\` to see open work.`
        : `Any task you delegated to them is now orphaned — call \`tasks_list\` and re-route or close.`);
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content: event.formatted_body ?? fallback,
        meta: { source: "inbetween", kind: event.type, chat_id: chatId, agent_id: agent.id },
      },
    });
    console.error(`[inbetween] ${verb} chat ${chatId}: @${handle}`);
  } catch (e) {
    console.error("[inbetween] notify member-change failed:", e);
  }
}

async function notifyClaudeAboutChatRemoval(chatId: number | string | null, serverRendered?: string | null): Promise<void> {
  try {
    const idText = chatId == null ? "?" : String(chatId);
    const fallback =
      `🚪 You were removed from chat #${idText}. ` +
      `Won't receive further messages from there. ` +
      `Stop responding for that chat_id.`;
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content: serverRendered ?? fallback,
        meta: { source: "inbetween", kind: "removed_from_chat", chat_id: chatId },
      },
    });
    console.error(`[inbetween] 🚪 removed from chat ${idText}`);
  } catch (e) {
    console.error("[inbetween] notify chat-removal failed:", e);
  }
}

async function notifyClaudeAboutWake(event: any): Promise<void> {
  try {
    const reason = event.reason ? `\nReason: ${event.reason}` : "";
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content: `⏰ Wake request received (id=${event.request_id}).${reason}\n\nUse the \`ack_wake\` tool with status='acknowledged' once you start handling it, then 'completed' when done.`,
        meta: { source: "agentgram", kind: "wake", request_id: event.request_id },
      },
    });
  } catch (e) {
    console.error("[inbetween] notify wake failed:", e);
  }
}

async function notifyClaudeAboutTask(event: any): Promise<void> {
  try {
    // Only `task_created` for standalone (no-chat) tasks reaches this path —
    // chat-bound task events arrive as system messages in the chat timeline.
    const fallback =
      `🗒 Task #${event.task_id} created by @${event.from_agent}: ${event.title}\n\n` +
      `Call \`tasks_list\` to see details. When you finish work on a task, ALWAYS call \`tasks_upsert(id=${event.task_id}, status="done")\` so it gets marked closed.`;
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content: event.formatted_body ?? fallback,
        meta: { source: "inbetween", kind: "task", task_id: event.task_id, task_event: event.type },
      },
    });
  } catch (e) {
    console.error("[inbetween] notify task failed:", e);
  }
}

// =================================================================
// POLLING FALLBACK (если WebSocket не работает)
// =================================================================
let lastSeenMessageId: string | null = null;

async function pollInbox(): Promise<void> {
  try {
    const result = await api<any>("GET", "/inbox?pending_only=true&limit=20");
    const messages: any[] = result.messages || [];

    for (const m of messages.reverse()) {
      // Дедуп через локальный inbox
      if (inbox.find((x) => x.message_id === m.message_id)) continue;

      const msg: Message = {
        message_id: m.message_id,
        from_agent: m.from_agent,
        content: m.content,
        attachments: m.attachments || [],
        metadata: { ...(m.metadata || {}), from_human: !!m.from_human },
        sent_at: m.sent_at,
      };
      inbox.unshift(msg);
      if (inbox.length > MAX_INBOX_SIZE) inbox.pop();

      await notifyClaudeAboutMessage(msg);
    }
  } catch (e) {
    // silent — обычная сетевая ошибка
  }
}

let pollingStarted = false;
function startPolling(): void {
  if (pollingStarted) return;
  pollingStarted = true;
  setInterval(pollInbox, 8000);
}

// =================================================================
// IDENTITY DROP — used by agent_logout tool
// =================================================================
function logoutAgent(): void {
  try { if (ws) { ws.removeAllListeners(); ws.close(); ws = null; } } catch (e) {}
  inbox.length = 0;
  console.error(`[inbetween] logout: @${activeAgentName || "?"} → idle`);
  activeAgentToken = null;
  activeAgentName = null;
  activeAgentId = null;
  clearSession();
  // Clear the per-chat context block so the previous agent's playbook/bio
  // doesn't survive into the logged-out (or next-agent) state. The
  // refresh-on-next-login will repopulate; until then, placeholder text.
  resetChatContext();
}

// =================================================================
// BACKEND API CLIENT
// =================================================================
async function api<T = any>(
  method: string,
  path: string,
  body?: any,
  opts?: { tokenOverride?: string }
): Promise<T> {
  // Token resolution: explicit override (used by owner_login validation) >
  // activeAgentToken (set by agent_login). Layer gates ensure we never get
  // here without a token; the explicit guard is just a defensive net.
  const tok = opts?.tokenOverride || activeAgentToken;
  if (!tok) {
    throw new Error("Not authenticated. Call owner_login(email, password) and then agent_login(token).");
  }
  const res = await signedFetch(`${BACKEND_URL}${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${tok}`,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    throw new Error(`API ${method} ${path}: ${res.status} ${await res.text()}`);
  }
  return res.json();
}

async function sendMessage(
  to_agent: string,
  content: string,
  attachments: any[] = [],
  metadata: any = {}
) {
  return api("POST", "/messages", {
    to_agent,
    content,
    attachments,
    metadata,
  });
}

async function fetchInbox(pending_only = false) {
  return api("GET", `/inbox?pending_only=${pending_only}`);
}

// === USER FEATURES (v0.0.8) ===
async function markRead(message_id: string) {
  return api("POST", `/messages/${encodeURIComponent(message_id)}/read`);
}
async function markAllRead() {
  return api("POST", "/messages/read_all");
}
async function getMessagesWith(with_agent: string, limit: number) {
  return api(
    "GET",
    `/messages?with_agent=${encodeURIComponent(with_agent)}&limit=${limit}`,
  );
}
// bio (self + coord-edits-other) lives in chat_members.bio and is set via
// set_chat_settings — there's no separate update_profile / set_member_bio
// surface anymore. The old global agents.description column is no longer
// written from MCP.
async function addChatMember(chat_id: string, agent_id: number) {
  return api(
    "POST",
    `/chats/${encodeURIComponent(chat_id)}/members`,
    { agent_id },
  );
}
async function removeChatMember(chat_id: string, agent_id: number) {
  return api(
    "DELETE",
    `/chats/${encodeURIComponent(chat_id)}/members/${agent_id}`,
  );
}
async function spawnAgentInChat(
  chat_id: string,
  display_name: string,
  bio?: string,
) {
  const body: any = { display_name };
  // Field name follows the backend CreateEphemeralAgentReq: `bio` lands in
  // chat_members.bio for the new member, not in the global agents.description.
  if (bio !== undefined) body.bio = bio;
  return api("POST", `/chats/${encodeURIComponent(chat_id)}/agents`, body);
}

// =================================================================
// LAUNCH NEW IDE WINDOW
// Used by `spawn_agent` with auto_launch=true to actually pop a fresh
// Claude Code window in a target folder, with the onboarding prompt
// already passed as the first positional arg. Claude Code accepts the
// initial prompt that way (`claude "<prompt>"`) — it stays interactive
// and the prompt becomes the first user message, which is exactly what
// makes the new agent autonomously call agent_login on startup.
//
// We never use `shell: true`; argv arrays go to spawn directly, so we
// only need per-platform string quoting (not shell-meta escaping).
// =================================================================

// LLMs love to JSON-stringify numeric ids ("42") even when the schema asks
// for a number — coerce both shapes here so the handler doesn't reject the
// call with a confusing "agent_id must be number" when the user clearly
// meant "agent 42".
function coerceAgentId(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v) && Number.isInteger(v)) return v;
  if (typeof v === "string") {
    const t = v.trim();
    if (/^\d+$/.test(t)) {
      const n = parseInt(t, 10);
      if (Number.isFinite(n)) return n;
    }
  }
  return null;
}

function shQuoteUnix(s: string): string {
  // Wrap in single quotes; embed internal single quotes via `'\''` dance.
  return "'" + s.replace(/'/g, "'\\''") + "'";
}

function shQuoteWindowsCmd(s: string): string {
  // cmd.exe: wrap in double quotes, escape internal `"` by doubling.
  return '"' + s.replace(/"/g, '""') + '"';
}

function appleScriptEscape(s: string): string {
  // Embedding inside an AppleScript double-quoted string literal —
  // escape backslashes first, then quotes.
  return s.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

interface LaunchResult {
  ok: boolean;
  platform: NodeJS.Platform;
  command_summary?: string;
  error?: string;
}

function launchClaudeInNewWindow(
  cwd: string,
  prompt: string,
): LaunchResult {
  const platform = process.platform;
  try {
    if (platform === "win32") {
      // Windows console spawn — three landmines stacked on top of each other:
      //
      //   1. Node's default Windows arg escaping uses CRT rules (backslash-
      //      quote pairs). cmd.exe parses quotes differently and treats the
      //      backslash as literal, so a path that goes through default
      //      spawn ends up looking like `cd /d \"C:\…\"` to cmd — which
      //      throws "Синтаксическая ошибка в имени файла". Fix:
      //      windowsVerbatimArguments=true → Node passes our args verbatim,
      //      cmd sees exactly what we wrote.
      //
      //   2. Doing `cd /d <path> && claude …` inside a cmd /k means cmd has
      //      to parse a nested quoted path before claude runs. Skip that
      //      whole layer: `start /D <path>` natively sets the new process's
      //      working directory, so the inner cmd /k just runs `claude …`.
      //
      //   3. `start ""` (empty title) is still required so start doesn't
      //      mistake the next quoted token for a window title.
      //
      // The prompt itself is plain ASCII with single quotes / spaces / dashes
      // (no `"` after we dropped the auth_token="…" form, no &/|/<>/() either),
      // so wrapping it in plain double quotes is safe — cmd's two-quote
      // rule preserves them unmodified.
      const quotedCwd = `"${cwd.replace(/"/g, '""')}"`;
      const quotedPrompt = `"${prompt.replace(/"/g, '""')}"`;
      childSpawn(
        "cmd",
        ["/c", "start", '""', "/D", quotedCwd, "cmd", "/k", `claude ${quotedPrompt}`],
        { detached: true, stdio: "ignore", windowsVerbatimArguments: true },
      ).unref();
      return {
        ok: true,
        platform,
        command_summary: `cmd /c start "" /D ${quotedCwd} cmd /k claude ${quotedPrompt}`,
      };
    }
    if (platform === "darwin") {
      const inner = `cd ${shQuoteUnix(cwd)} && claude ${shQuoteUnix(prompt)}`;
      const apple = `tell application "Terminal" to do script "${appleScriptEscape(inner)}"`;
      childSpawn("osascript", ["-e", apple], { detached: true, stdio: "ignore" }).unref();
      return { ok: true, platform, command_summary: `osascript -e '${apple}'` };
    }
    if (platform === "linux") {
      // gnome-terminal is the most common default on desktop Linux. If it's
      // missing the spawn will fail with ENOENT and we fall back to the
      // manual-paste path on the caller side.
      const inner = `claude ${shQuoteUnix(prompt)}; exec bash`;
      childSpawn(
        "gnome-terminal",
        ["--working-directory", cwd, "--", "bash", "-c", inner],
        { detached: true, stdio: "ignore" },
      ).unref();
      return { ok: true, platform, command_summary: `gnome-terminal --working-directory ${cwd} -- bash -c '${inner}'` };
    }
    return { ok: false, platform, error: `unsupported platform: ${platform}` };
  } catch (e: any) {
    return { ok: false, platform, error: e?.message || String(e) };
  }
}
async function blockAgent(name: string) {
  return api("POST", `/agents/${encodeURIComponent(name)}/block`);
}
async function unblockAgent(name: string) {
  return api("DELETE", `/agents/${encodeURIComponent(name)}/block`);
}

// === V0.2 — wake (#5), tasks (#6) ===
// Legacy prompt-prefs API (set_global_prompt / set_chat_prompt / persona) was
// removed: never had a UI hook, never called by anybody. The single source of
// truth for system prompts now is `chat_members.instructions` ("Private
// playbook") set via the chat settings modal — backend mirrors it back into
// every push as the [System context] block.
async function listTasks(status?: string, limit = 50) {
  const qs = status ? `?status=${encodeURIComponent(status)}&limit=${limit}` : `?limit=${limit}`;
  return api("GET", `/tasks${qs}`);
}
async function createTask(payload: {
  title: string;
  description?: string;
  due?: string;
  agent_name?: string;
  assignee?: string;
  chat_id?: number;
  completion_note?: string;
}) {
  return api("POST", "/tasks", payload);
}
async function updateTask(id: number, payload: any) {
  return api("PATCH", `/tasks/${id}`, payload);
}
async function deleteTask(id: number) {
  return api("DELETE", `/tasks/${id}`);
}

// === Checkpoints — short "what + why" notes a project leaves in a chat ===
async function createCheckpoint(payload: { chat_id: number; title: string; summary: string }) {
  return api("POST", "/checkpoints", payload);
}
async function listCheckpoints(chat_id: number, limit = 50) {
  return api("GET", `/checkpoints?chat_id=${chat_id}&limit=${limit}`);
}

// === Scheduled pings — one-shot delayed message in a chat ===
async function createScheduledPing(payload: { chat_id: number; content: string; delay_seconds: number }) {
  return api("POST", "/scheduled_pings", payload);
}
async function listScheduledPings(chat_id: number) {
  return api("GET", `/scheduled_pings?chat_id=${chat_id}`);
}
async function cancelScheduledPing(id: number) {
  return api("DELETE", `/scheduled_pings/${id}`);
}

// === Unified chats ===
async function listChats() {
  return api("GET", "/chats");
}
async function chatMessages(opts: {
  chat_id: string;
  limit?: number;
  before?: string;
  since?: string;
  until?: string;
  unread?: boolean;
}) {
  const qs = new URLSearchParams();
  if (opts.limit != null) qs.set("limit", String(opts.limit));
  if (opts.before) qs.set("before", opts.before);
  if (opts.since) qs.set("since", opts.since);
  if (opts.until) qs.set("until", opts.until);
  if (opts.unread) qs.set("unread", "true");
  const path = `/chats/${encodeURIComponent(opts.chat_id)}/messages?${qs.toString()}`;
  return api("GET", path);
}
async function chatMarkRead(chat_id: string) {
  return api("POST", `/chats/${encodeURIComponent(chat_id)}/read`);
}
async function chatSend(
  chat_id: string,
  content: string,
  attachments: any[] = [],
  metadata: any = {},
  reply_to_message_id?: string,
) {
  const body: any = { content, attachments, metadata };
  if (reply_to_message_id) body.reply_to_message_id = reply_to_message_id;
  return api("POST", `/chats/${encodeURIComponent(chat_id)}/messages`, body);
}
async function chatThread(chat_id: string, root_message_id: string) {
  return api(
    "GET",
    `/chats/${encodeURIComponent(chat_id)}/thread/${encodeURIComponent(root_message_id)}`,
  );
}
async function searchMessages(opts: {
  q: string; limit?: number; since?: string; until?: string; chat_id?: string;
}) {
  const qs = new URLSearchParams({ q: opts.q });
  if (opts.limit != null) qs.set("limit", String(opts.limit));
  if (opts.since) qs.set("since", opts.since);
  if (opts.until) qs.set("until", opts.until);
  if (opts.chat_id) qs.set("chat_id", opts.chat_id);
  return api("GET", `/messages/search?${qs.toString()}`);
}
async function getChat(chat_id: string) {
  return api("GET", `/chats/${encodeURIComponent(chat_id)}`);
}
async function setChatSettings(opts: {
  chat_id: string;
  notify_mode?: "always" | "on_mention" | "never";
  bio?: string;
  instructions?: string;
  agent_id?: number;
}) {
  const body: any = {};
  if (opts.notify_mode !== undefined) body.notify_mode = opts.notify_mode;
  if (opts.bio !== undefined) body.bio = opts.bio;
  if (opts.instructions !== undefined) body.instructions = opts.instructions;
  if (opts.agent_id !== undefined) body.agent_id = opts.agent_id;
  return api("PATCH", `/chats/${encodeURIComponent(opts.chat_id)}/membership`, body);
}
async function inboxUnread(opts: { limit?: number; since?: string } = {}) {
  const qs = new URLSearchParams();
  if (opts.limit != null) qs.set("limit", String(opts.limit));
  if (opts.since) qs.set("since", opts.since);
  const path = `/inbox/unread${qs.toString() ? "?" + qs.toString() : ""}`;
  return api("GET", path);
}

// =================================================================
// MCP SERVER SETUP
// =================================================================
const server = new Server(
  { name: "inbetween", version: "0.1.0" },
  {
    instructions:
      "InBetween — direct line between AI agents working as the owner's team. Treat the chat like a Telegram group with motivated teammates, not a corporate channel.\n\n" +
      "1. ALWAYS reply via `chat_send`. The InBetween chat is the ONLY surface the owner sees — console output is invisible to him, so a console-only reply is silence. Console may follow chat_send for IDE UX, never replace it.\n\n" +
      "2. Teammate tone, save the owner's tokens. Short, plain, casual — like coworkers in a group chat, not a status report. No corporate/formal phrasing, no posturing. No sycophancy: drop \"you're absolutely right\" / \"great point\" — say what's true or just act. When work is done, say \"done\" and stop; don't pad with extra suggestions. One substantive message beats five tiny pings; bundle clarification questions into one message, not three. Multiple `chat_send` calls ARE fine when each carries real content for different recipients (e.g. coordinator delegating to several agents at once). Other agents have the same tools and intelligence as you — treat them as equals, don't think for them, don't redo their lane.\n\n" +
      "3. Honesty over agreement. Push back when you disagree (with the owner OR another agent); don't validate to be polite. Don't claim \"done\" until you actually ran/checked it — if verification wasn't possible, say so plainly. State load-bearing assumptions out loud before non-trivial work. If you spot something that affects another agent (changed contract, new error path, blocker upstream), drop a one-liner in chat unprompted.\n\n" +
      "4. Wake-up routing. The server already filtered who to wake; if you got pushed, READ the message — even if your @handle isn't spelled exactly (someone may have used your old nick). If it's clearly for someone else, stay quiet (coordinator routes unaddressed traffic); if it's for you, reply.\n\n" +
      "5. Stay in lane; don't block, don't collide. Designer → design, backend → backend, marketer → marketing, etc. If a needed role isn't covered in the chat, agree who picks it up; coordinators can spawn an agent for the gap and should delegate SCOPE, not steps (goal + acceptance criterion, trust the specialist with the algorithm). If you're about to build something that conflicts with another agent's approach, escalate to the coordinator/owner BEFORE both implement. Other agents may be offline/busy/broken — do everything in your power in parallel, escalate only when truly stuck. Some members in `list_agents` carry a `linked_chat_id` — those are linked-group channels (a whole other chat acting as one teammate). Mention them like any other agent; reply directly to their bubble (`reply_to_message_id`) and it bounces back to their team automatically. No `become_agent`, no extra steps.\n\n" +
      "6. Visibility on long work + clean handoffs. Don't go dark — on tasks longer than a few minutes, post one mid-progress update (done / left / blockers). When you delegate, include concrete context (file path, error text, constraint), not \"see above\". When you finish delegated work, name the artifact (branch, commit, file), not just \"done\".\n\n" +
      "7. Local-file safety. Before touching files on disk, confirm which folder & machine each agent is on. Two agents in the same cwd on the same host WILL clobber each other. Ask in chat if unsure, and publish your own cwd via `set_chat_settings(bio=...)` so other agents can see it.\n\n" +
      "8. Track work via tasks. BEFORE non-trivial work — `tasks_upsert(title=..., status=\"todo\", chat_id=<chat>)`. ON finish — `tasks_upsert(id=..., status=\"done\")`. WHEN delegating to another agent — `tasks_upsert(title=..., assignee=\"<their handle>\", chat_id=<chat>)` BEFORE @-mentioning them. Note: assignees are NOT pushed personally — they see the assignment as a system event in the chat. ON entering a chat or after restart — `tasks_list`. Coordinators delegate often; this is mandatory for them.\n\n" +
      "9. Files in pushes appear as a `📎 N attachments:` block — call `attachment_download(message_id, index)` for a fresh 10-min signed URL, then WebFetch the bytes. To send a file: `attachment_send(chat_id, content, local_path)` (uploads + posts atomically; ≤25MB; image/png|jpeg|webp|gif, application/pdf|json, text/plain|markdown).",
    capabilities: {
      // listChanged: true — server announces that it will emit
      // notifications/tools/list_changed when tool descriptions update.
      // Required so Claude Code re-fetches the tools list; without this
      // declaration the client may ignore the notification and the
      // `inbetween_chat_context` description stays stale.
      tools: { listChanged: true },
      resources: {
        subscribe: true,
        listChanged: true,
      },
      logging: {},
      // Claude Code Channels — позволяет server'у инжектить сообщения прямо
      // в открытую сессию юзера через notifications/claude/channel.
      // Требует: feature flag tengu_harbor у юзера + либо approved allowlist,
      // либо запуск claude с флагом --dangerously-load-development-channels.
      experimental: {
        "claude/channel": {},
        "claude/channel/permission": {},
      },
    },
  }
);

// === TOOLS ===
server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    // ===== Persistent chat-context block =====
    // No-op tool whose *description* is the per-chat state (role, bio,
    // playbook) for the logged-in agent. Refreshed dynamically — see
    // refreshChatContext() / notifications/tools/list_changed.
    {
      name: "inbetween_chat_context",
      description: chatContextDescription,
      inputSchema: { type: "object", properties: {} },
    },
    // ===== Layer 0 — owner-level auth =====
    {
      name: "owner_login",
      description:
        "Authenticate this MCP session as a human owner using your inbetween.chat email and password. MUST be called before any other tool — agent_login and chat operations are gated behind this. The credentials are exchanged for a long-lived owner token that persists to ~/.inbetween/owner.json so future MCP boots pick it up automatically. Email and password are never written to disk.",
      inputSchema: {
        type: "object",
        properties: {
          email: { type: "string", description: "Email of your inbetween.chat account" },
          password: { type: "string", description: "Password of your inbetween.chat account" },
        },
        required: ["email", "password"],
      },
    },
    {
      name: "owner_logout",
      description:
        "Drop the owner session. After this all tools are blocked until owner_login is called again. Also clears any active agent_login.",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "whoami",
      description:
        "Show who this MCP session is currently logged in as. Returns owner status, active agent (if any), and current chat context.",
      inputSchema: { type: "object", properties: {} },
    },
    // ===== Layer 1 — agent identity inside the chat =====
    {
      name: "agent_login",
      description:
        "Become a specific agent inside a chat. Paste the auth_token from the chat onboarding prompt (the one you copied from inbetween.chat when an agent was spawned). After this, all chat tools act as that agent. Requires owner_login first. The identity is saved to ~/.inbetween/sessions/ so MCP and the Codex live-push wrapper can restore it across restarts.",
      inputSchema: {
        type: "object",
        properties: {
          auth_token: { type: "string", description: "Agent auth token from the chat onboarding prompt" },
        },
        required: ["auth_token"],
      },
    },
    {
      name: "agent_logout",
      description:
        "Drop the current agent identity. The owner session stays active — you can call agent_login with a different agent token next.",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "add_chat_member",
      description:
        "Coordinator-only: pull an EXISTING agent into this chat. The agent must already exist (e.g. one of your owner's other agents discoverable via list_agents at the owner level). For brand-new agents use `spawn_agent` instead. Backend rejects this call if you are not the coordinator of `chat_id`.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string", description: "Chat where you are the coordinator." },
          agent_id: {
            type: ["number", "string"],
            description: "Existing agent's id (integer; string-integer also accepted). Discover via `list_agents` if you don't know it.",
          },
        },
        required: ["chat_id", "agent_id"],
      },
    },
    {
      name: "remove_chat_member",
      description:
        "Remove an agent from this chat. Either you (any agent removing yourself) or coordinator-only when removing somebody else. If the target is ephemeral and this was their last chat they're soft-deleted server-side. Use this to kick an unresponsive teammate after a delegation timed out, then `spawn_agent` a replacement.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string", description: "Chat id." },
          agent_id: {
            type: ["number", "string"],
            description: "Agent to remove (integer; string-integer also accepted). Pass your own id to leave. Discover via `list_agents`.",
          },
        },
        required: ["chat_id", "agent_id"],
      },
    },
    {
      name: "spawn_agent",
      description:
        "Coordinator-only: create a brand-new ephemeral agent and add them to this chat in one call. The new agent inherits ownership from your owner, so the human behind you can see and manage them in the web UI like any of their own.\n\n" +
        "Set `auto_launch=true` to actually pop a NEW Claude Code window on the host machine in `work_dir`, with the onboarding prompt already passed as the first message — the new instance authenticates itself via agent_login on startup and shows up online without the human paste'ing anything. Falls back to manual-paste output if the launch fails (e.g. no graphical terminal available).\n\n" +
        "`target` selects the IDE binary to launch. Only `\"claude\"` is wired up; `\"codex\"` is reserved for future support and currently returns the prompt for manual paste.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string", description: "Chat where you are the coordinator." },
          display_name: { type: "string", description: "User-visible handle for the new agent (must be unique in this chat)." },
          bio: { type: "string", description: "Optional initial bio for the new agent." },
          auto_launch: {
            type: "boolean",
            description: "If true, attempt to spawn a new IDE window in `work_dir` with the onboarding prompt pre-loaded. Default false — returns the prompt for manual paste.",
            default: false,
          },
          target: {
            type: "string",
            enum: ["claude", "codex"],
            description: "IDE binary to launch when auto_launch=true. Only \"claude\" is implemented; \"codex\" falls back to manual paste.",
            default: "claude",
          },
          work_dir: {
            type: "string",
            description: "Absolute path of the folder the new IDE window should open in. Required when auto_launch=true.",
          },
        },
        required: ["chat_id", "display_name"],
      },
    },
    // ===== #6, #11 — tasks =====
    {
      name: "tasks_list",
      description:
        "List YOUR tasks (where you are owner OR assignee). CALL THIS when entering a chat where you are a member, when you receive a `task_*` system event, or before starting any work the owner asked for — to make sure nothing is already tracked. Filter by status: todo|in_progress|done.",
      inputSchema: {
        type: "object",
        properties: {
          status: { type: "string", enum: ["todo", "in_progress", "done"] },
          limit: { type: "number", default: 50 },
        },
      },
    },
    {
      name: "tasks_upsert",
      description:
        "Create or update a task. CALL THIS in three situations: (1) before you start a piece of work — create a `todo` task so the chat can see what you're doing; (2) when you finish — update with status='done' so members see it closed; (3) when you delegate work to another agent — create a task with `assignee` (their @-handle) so the chat sees them assigned. If `id` is set → update; if `id` is omitted → create. Tasks bound to `chat_id` appear as system events in the chat timeline (task_created / task_assigned / task_updated / task_done / task_deleted) — assignees are NOT pushed personally, they see the event in the chat like everyone else.",
      inputSchema: {
        type: "object",
        properties: {
          id: { type: "number", description: "Task id to update; omit to create" },
          title: { type: "string" },
          description: { type: "string", description: "Free-text body of the task" },
          status: { type: "string", enum: ["todo", "in_progress", "done"] },
          due: { type: "string", description: "Date in YYYY-MM-DD form (no time component)" },
          agent_name: { type: "string", description: "Owner agent of the task (create only; default: yourself)" },
          assignee: { type: "string", description: "Executor agent's @-handle (string). Pass empty string to unassign on PATCH." },
          chat_id: { type: "number", description: "Attach task to a chat (optional but recommended — without it the task is invisible to chat members)" },
          completion_note: { type: "string", description: "Optional 'what was done' note when closing a task (≤1000 chars)" },
        },
      },
    },
    // ===== Checkpoints — project journal =====
    {
      name: "checkpoint_create",
      description:
        "Drop a short 'what + why' record on the project log of a chat. NOT for every keystroke — call this when something substantive happened: a feature shipped, a migration applied, an architecture decision made, a bug investigated. The chat sees a quiet system event in the timeline; the full text lives in the checkpoint store and is fetched via `checkpoints_list`. Use this so other agents (and the human owner) entering the chat days later can quickly understand the course of the project without scrolling through every message.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "number", description: "Chat where the work happened" },
          title: { type: "string", description: "One-line headline (≤256 chars). Example: 'Migration 037 shipped' / 'Auth rewrite — picked option B'" },
          summary: { type: "string", description: "Multi-line body — what was done and (importantly) WHY. Future-you / other agents read this to catch up." },
        },
        required: ["chat_id", "title", "summary"],
      },
    },
    {
      name: "checkpoints_list",
      description:
        "Read the project log for a chat (newest first). CALL THIS when you enter a chat after a long pause, when the owner asks 'what's been happening here', or before making a big change so you don't undo a decision a previous agent already made. Returns title + summary + author + timestamp.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "number" },
          limit: { type: "number", default: 50, description: "Max entries (cap 200)" },
        },
        required: ["chat_id"],
      },
    },
    // ===== Scheduled pings — wake the chat later =====
    {
      name: "schedule_ping",
      description:
        "Schedule a chat message to be sent later by YOU (the active agent). Use this when you're about to end your session but you (or other agents) should pick the work up after a wait — e.g. 'a build is running, ping back in 30 minutes', 'partner team will reply by EoD, prod me tomorrow morning'. When `run_at` hits, the backend posts `content` as if you typed it in chat_send right now — same @-mention routing wakes the targeted agents. Use `@all` to wake everyone, or `@<display_name>` to wake specific agents. `delay_seconds` is 10s minimum, 24h (86400) maximum.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "number", description: "Chat to ping" },
          delay_seconds: { type: "number", description: "How many seconds from now until the message fires (10–86400)" },
          content: { type: "string", description: "Message text — include @<handle> or @all to wake recipients on fire" },
        },
        required: ["chat_id", "delay_seconds", "content"],
      },
    },
    {
      name: "list_scheduled_pings",
      description:
        "List pending scheduled pings for a chat (yours and others'). Returns the ones that haven't fired yet, ordered by run_at ascending. Useful to check what's already queued so you don't double-schedule, and to find the ping_id if you want to cancel.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "number" },
        },
        required: ["chat_id"],
      },
    },
    {
      name: "cancel_scheduled_ping",
      description:
        "Cancel a pending ping you scheduled earlier. Only the original scheduler can cancel. Already-fired pings can't be cancelled — they've shipped.",
      inputSchema: {
        type: "object",
        properties: {
          id: { type: "number", description: "ping_id from `list_scheduled_pings`" },
        },
        required: ["id"],
      },
    },
    // ===== Unified chats =====
    {
      name: "list_chats",
      description:
        "List all your chats with last message, unread count, and other-side online status. Sorted by recency.",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "chat_messages",
      description:
        "Get messages from a specific chat with optional filters: limit, before (cursor), since/until (ISO timestamps), unread (only my unread). For full-text search use `search_messages` — it accepts an optional chat_id filter and does the same thing more directly.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string", description: "Chat id from list_chats (number for direct, 'group:<uuid>' for legacy group)" },
          limit: { type: "number", default: 50 },
          before: { type: "string", description: "message_id cursor — returns messages older than this one" },
          since: { type: "string", description: "ISO 8601 — only messages at or after" },
          until: { type: "string", description: "ISO 8601 — only messages strictly before" },
          unread: { type: "boolean", description: "Only my unread messages", default: false },
        },
        required: ["chat_id"],
      },
    },
    {
      name: "chat_send",
      description:
        "Call this for EVERY reply to an InBetween push. This is the only surface visible to the owner — console output is NOT shown in the InBetween UI, so a console-only reply is treated as silence. Console output may follow chat_send for IDE UX, but chat_send must come first.\n\n" +
        "Routing for live push is parsed from @-mentions in the message content (the message is always saved for every member regardless):\n" +
        "  - mention `@all` → live push to every member except the sender\n" +
        "  - mention one or more `@<display_name>` → live push only to those agents. Use list_agents (or get_chat) to see each member's exact display_name.\n" +
        "  - no mentions → push only to the chat's coordinator (or nobody if no coordinator)\n" +
        "Use `@all` for announcements. Use specific `@<display_name>` mentions to delegate work. Omit mentions to let the coordinator triage.\n\n" +
        "**Files:** to send a file along with the message, use `attachment_send` instead — it uploads the file from disk and posts the message in one call. The plain `chat_send` is text-only.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string", description: "Chat id (from list_chats)" },
          content: { type: "string", description: "Message text. Include @<display_name> or @all to control routing." },
          attachments: { type: "array", items: { type: "object" }, description: "Pre-uploaded attachment objects (rare — usually you should call `attachment_send` instead which handles upload + send atomically)." },
          reply_to_message_id: {
            type: "string",
            description: "Optional UUID of a parent message to reply to. The new message links to that parent and inherits its thread root, so `chat_thread` can return them together.",
          },
        },
        required: ["chat_id", "content"],
      },
    },
    {
      name: "chat_thread",
      description:
        "Fetch every message in a single reply thread, in chronological order. Pass `root_message_id` — the UUID of the FIRST message in the thread (either the literal root, or any reply's `root_message_id` field). Returns the root plus every reply, flat. Useful when you receive a push that references an earlier message and you need the conversation context.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string", description: "Chat id." },
          root_message_id: { type: "string", description: "UUID of the thread's root message." },
        },
        required: ["chat_id", "root_message_id"],
      },
    },
    // ===== Attachments =====
    {
      name: "attachment_download",
      description:
        "Download an attachment from a message you've received. When a push arrives with a `📎 N attachments:` block, this tool fetches a fresh signed URL (TTL 10 min) for the file you want. Use the returned `download_url` with WebFetch (or any HTTP client) to grab the bytes — it's a regular Supabase-Storage signed URL, no auth header needed during the TTL window. The file lives in a private bucket gated by RLS — only chat members can sign for it.",
      inputSchema: {
        type: "object",
        properties: {
          message_id: { type: "string", description: "message_id from the push or from chat_messages" },
          index: { type: "number", default: 0, description: "Which attachment in the message (0 = first). Most messages carry exactly one." },
        },
        required: ["message_id"],
      },
    },
    {
      name: "attachment_send",
      description:
        "Upload a local file AND post a chat message with it attached, atomically in a single call. Use this whenever you want to share an artifact (screenshot, log file, generated PDF, JSON dump, etc.) — saves you the dance of separately uploading and then attaching. Limits: ≤25MB; allowed MIME types are image/png|jpeg|webp|gif, application/pdf|json|octet-stream, text/plain|markdown. Storage is a private bucket — only members of the target chat can fetch what you upload.\n\n" +
        "Routing rules for the message text are the same as `chat_send`: include `@<display_name>` mentions to wake specific agents, `@all` for everyone, no mention = coordinator triages.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string", description: "Chat id (from list_chats)" },
          content: { type: "string", description: "Message text accompanying the attachment. May contain @-mentions." },
          local_path: { type: "string", description: "Absolute or cwd-relative path to the file on disk you want to send." },
          name: { type: "string", description: "Optional override for the visible file name (defaults to the basename of local_path)." },
        },
        required: ["chat_id", "content", "local_path"],
      },
    },
    {
      name: "chat_mark_read",
      description: "Mark an entire chat as read up to now (moves your last_read_at cursor).",
      inputSchema: {
        type: "object",
        properties: { chat_id: { type: "string" } },
        required: ["chat_id"],
      },
    },
    {
      name: "search_messages",
      description:
        "Full-text search across all messages you have access to. Supports websearch syntax: \"exact phrase\", word OR word, -excluded. Returns highlighted snippets.",
      inputSchema: {
        type: "object",
        properties: {
          q: { type: "string" },
          limit: { type: "number", default: 50 },
          since: { type: "string" },
          until: { type: "string" },
          chat_id: { type: "string", description: "Restrict search to a specific chat" },
        },
        required: ["q"],
      },
    },
    {
      name: "get_chat",
      description:
        "Get chat metadata + my per-member settings (notify_mode, chat_prompt) and member list. Includes coordinator_agent_id at chat level and is_coordinator on each member.",
      inputSchema: {
        type: "object",
        properties: { chat_id: { type: "string" } },
        required: ["chat_id"],
      },
    },
    {
      name: "list_agents",
      description:
        "List the agents that are members of a chat, with display names, online status, and which one is the coordinator. Same data as get_chat.members but without your private settings.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string", description: "Chat id from list_chats" },
        },
        required: ["chat_id"],
      },
    },
    {
      name: "set_chat_settings",
      description:
        "Configure per-chat settings. Three independent fields:\n" +
        "• notify_mode: 'always' | 'on_mention' | 'never' — when this chat pushes the target.\n" +
        "• bio: short PUBLIC card visible to other members; describes what the target does in this chat.\n" +
        "• instructions: PRIVATE playbook only the target sees; prepended every time they read this chat.\n\n" +
        "By default edits YOUR OWN row in this chat. Pass `agent_id` to edit somebody else's row — backend allows this only if you are the coordinator of `chat_id` (otherwise the call silently falls back to editing yourself, so coord checks must happen client-side if precision matters).",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string" },
          notify_mode: { type: "string", enum: ["always", "on_mention", "never"] },
          bio: {
            type: "string",
            description: "Public — visible to other chat members. Empty string clears it.",
          },
          instructions: {
            type: "string",
            description: "Private — only the target sees this. Empty string clears it.",
          },
          agent_id: {
            type: ["number", "string"],
            description: "Optional target agent (integer; string-integer also accepted). Omit to edit your own row. When provided, requires you to be the coordinator of `chat_id`; non-coordinators silently end up editing themselves instead. Discover via `list_agents`.",
          },
        },
        required: ["chat_id"],
      },
    },
    {
      name: "inbox_unread",
      description:
        "Get all my unread messages across every chat, newest first. Optionally filter by `since` timestamp.",
      inputSchema: {
        type: "object",
        properties: {
          limit: { type: "number", default: 50 },
          since: { type: "string", description: "ISO 8601 — only unread at or after" },
        },
      },
    },
  ],
}));

// Tool name → required layer.
// inbetween_chat_context is a description-only tool; calling it is a no-op
// but allowed at any layer so Claude can re-read the block even before
// agent_login (placeholder text guides the user to log in).
const LAYER0_TOOLS = new Set([
  "owner_login", "owner_logout", "whoami",
  "inbetween_chat_context",
]);
const LAYER1_TOOLS = new Set(["agent_login", "agent_logout"]);

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;
  const callId = Math.random().toString(36).slice(2, 8);
  console.error(`[inbetween] TOOL_CALL start id=${callId} name=${name}`);

  // Gate: every Layer-2 tool (everything not in LAYER0/LAYER1) requires
  // both owner_login AND agent_login. We check up-front so each handler
  // body can assume identity is set.
  if (!LAYER0_TOOLS.has(name) && !LAYER1_TOOLS.has(name)) {
    const err = requireAgent();
    if (err) {
      console.error(`[inbetween] TOOL_CALL gate-fail id=${callId} name=${name} err=${err}`);
      return { content: [{ type: "text", text: `✗ ${err}` }], isError: true };
    }
  }

  // ===== Persistent context (no-op) =====
  // The data is the description in the tools list. Returning it here lets
  // Claude force-read the block on demand (e.g. after a long quiet period
  // where the system context may have been compacted out).
  if (name === "inbetween_chat_context") {
    return { content: [{ type: "text", text: chatContextDescription }] };
  }

  // ===== Layer 0 — owner auth =====
  if (name === "owner_login") {
    const email = typeof args?.email === "string" ? (args.email as string).trim() : "";
    const password = typeof args?.password === "string" ? (args.password as string) : "";
    if (!email || !password) {
      return { content: [{ type: "text", text: "✗ email and password required" }], isError: true };
    }
    if (!email.includes("@")) {
      return { content: [{ type: "text", text: "✗ email looks invalid" }], isError: true };
    }
    let owner_token: string | undefined;
    let owner_id: string | undefined;
    try {
      const res = await signedFetch(`${BACKEND_URL}/auth/cli-login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
      if (!res.ok) {
        let detail = `${res.status} ${res.statusText}`;
        try {
          const body: any = await res.json();
          if (body?.detail) detail = body.detail;
        } catch {}
        return { content: [{ type: "text", text: `✗ Login failed: ${detail}` }], isError: true };
      }
      const data: any = await res.json();
      owner_token = data?.owner_token;
      owner_id = data?.owner_id;
    } catch (e: any) {
      return { content: [{ type: "text", text: `✗ Login failed: ${e?.message || e}` }], isError: true };
    }
    if (!owner_token) {
      return { content: [{ type: "text", text: "✗ Login response missing owner_token" }], isError: true };
    }
    activeOwnerToken = owner_token;
    activeOwnerId = owner_id ?? null;
    saveOwner(owner_token, owner_id);
    return {
      content: [{
        type: "text",
        text: `✓ Signed in as owner. Session persisted to ~/.inbetween/owner.json.\nNext: paste an agent onboarding prompt (or call agent_login(token)) to start acting as an agent in a chat.`,
      }],
    };
  }
  if (name === "owner_logout") {
    // Best-effort server-side revoke before clearing local state — if a
    // leaked owner.json is the threat, local-only cleanup leaves the token
    // alive on the backend. Network failures don't block the local logout.
    const tokenToRevoke = activeOwnerToken;
    if (tokenToRevoke) {
      try {
        await signedFetch(`${BACKEND_URL}/auth/cli-logout`, {
          method: "POST",
          headers: { Authorization: `Bearer ${tokenToRevoke}` },
        });
      } catch (e: any) {
        console.error(`[inbetween] cli-logout server-side revoke failed: ${e?.message || e}`);
      }
    }
    activeOwnerToken = null;
    activeOwnerId = null;
    activeAgentToken = null;
    activeAgentName = null;
    activeAgentId = null;
    clearOwner();
    clearSession();
    try { if (ws) { ws.removeAllListeners(); ws.close(); ws = null; } } catch {}
    // Drop the cached per-chat context — owner_logout cascades to agent
    // identity, so the previous agent's data must not bleed into a
    // subsequent owner_login → agent_login as a different person.
    resetChatContext();
    return {
      content: [{ type: "text", text: "✓ Owner and agent sessions cleared (server-side token revoked). Call owner_login to authenticate again." }],
    };
  }
  if (name === "whoami") {
    const state: any = {
      owner: !!activeOwnerToken,
      owner_id: activeOwnerId,
      agent: activeAgentToken ? {
        agent_id: activeAgentId,
        name: activeAgentName,
      } : null,
    };
    // Best-effort: surface owner token expiry so Claude/Codex can warn when
    // re-login is approaching. Backend ground truth — don't trust local cache.
    if (activeOwnerToken) {
      try {
        const res = await signedFetch(`${BACKEND_URL}/auth/whoami`, {
          headers: { Authorization: `Bearer ${activeOwnerToken}` },
        });
        if (res.ok) {
          const w: any = await res.json();
          if (w?.expires_at) {
            const days = Math.round(
              (new Date(w.expires_at).getTime() - Date.now()) / (1000 * 60 * 60 * 24),
            );
            state.token_expires_at = w.expires_at;
            state.token_expires_in_days = days;
            if (days < 0) state.token_expired = true;
          }
        }
      } catch {}
    }
    if (!activeOwnerToken) state.note = "Not authenticated. Call owner_login(email, password) first.";
    else if (!activeAgentToken) state.note = "Owner authenticated. Now paste an agent onboarding prompt or call agent_login(token).";
    return { content: [{ type: "text", text: JSON.stringify(state, null, 2) }] };
  }

  // ===== Layer 1 — agent auth =====
  if (name === "agent_login") {
    const ownerErr = requireOwner();
    if (ownerErr) return { content: [{ type: "text", text: `✗ ${ownerErr}` }], isError: true };
    const tok = args?.auth_token as string;
    if (!tok || typeof tok !== "string") {
      return { content: [{ type: "text", text: "✗ auth_token required" }], isError: true };
    }
    let profile: any;
    try {
      profile = await api("GET", "/agents/whoami", undefined, { tokenOverride: tok });
    } catch (e: any) {
      return { content: [{ type: "text", text: `✗ Invalid agent token: ${e.message || e}` }], isError: true };
    }
    // display_name is the user-visible identifier; agents.name is internal
    // (Phase 2 will drop it). Fall back to name only if display_name is
    // somehow missing — shouldn't happen after migration 029.
    const visibleName = profile.display_name || profile.name;
    activeAgentToken = tok;
    activeAgentName = visibleName;
    activeAgentId = profile.id ?? profile.agent_id ?? null;
    // Always write session files. The default file is what `inbetween-codex`
    // watches to learn the active agent. Per-process file lets concurrent
    // Claude windows in the same folder keep separate identities.
    saveSession(tok, visibleName, activeAgentId);
    try { if (ws) { ws.removeAllListeners(); ws.close(); ws = null; } } catch {}
    // Wipe stale context BEFORE the async refresh fires — if this is a
    // token swap on top of an already-logged-in session, the description
    // currently shows the previous agent's playbook/role. Reset first so
    // the new agent never sees a stranger's data, even briefly.
    resetChatContext();
    connectWebSocket();
    // Populate the persistent chat-context tool description with this
    // agent's per-chat bio / playbook / coordinator status, and notify
    // Claude Code so it re-reads the tool list. Fire-and-forget — login
    // must succeed even if the context endpoint is briefly unhappy.
    refreshChatContext().catch((e) =>
      console.error(`[inbetween] post-login context refresh failed: ${e?.message || e}`),
    );
    return {
      content: [{
        type: "text",
        text:
          `✓ Acting as @${visibleName} (id=${activeAgentId}). Use list_chats to see chats you're in.\n\n` +
          `Your per-chat role, bio, and private playbook for every chat you're in are now exposed via the \`inbetween_chat_context\` tool's description — re-read that block whenever you respond, and it will refresh automatically when settings change.\n\n` +
          `**Team vibe + mandatory rules — read before replying:**\n` +
          `  1. Every reply to an InBetween push goes through \`chat_send\` first. Console output is invisible to the owner; a console-only reply = silence on his side.\n` +
          `  2. Teammate tone — short, plain, casual. No corporate phrasing, no sycophancy ("you're absolutely right", "great point" — drop it; say what's true or just act). When done, say "done" and stop; don't pad with extra suggestions. One substantive message beats five tiny pings; bundle clarification questions. Multiple chat_sends are fine when each carries real content for different recipients.\n` +
          `  3. Honesty: push back when you disagree (with owner OR another agent), don't validate to be polite. Don't claim "done" until you actually ran/checked it — say plainly if verification wasn't possible. State load-bearing assumptions out loud before non-trivial work. If you spot something that affects another agent (changed contract, new error path, blocker upstream), drop a one-liner in chat unprompted.\n` +
          `  4. Read every push even if @${visibleName} wasn't spelled exactly (e.g. old nick). Reply if for you; stay quiet if clearly for someone else (coordinator routes).\n` +
          `  5. Stay in lane; don't block; don't collide. Other agents have the same tools and intelligence as you — don't think for them, don't redo their lane. Do everything in your power in parallel — others may be offline/broken. If roles are missing, agree among yourselves; coordinators may spawn an agent for the gap and should delegate SCOPE (goal + acceptance criterion), not steps. If you're about to build something that conflicts with another agent's approach, escalate to coordinator/owner BEFORE both implement.\n` +
          `  6. Don't go dark on long work — post one mid-progress update (done / left / blockers). Handoffs include concrete context (path/error/constraint), not "see above". On finish of delegated work, name the artifact (branch / commit / file), not just "done".\n` +
          `  7. Before touching local files, confirm cwd & host — same cwd + same host = clobber risk. Publish your cwd via \`set_chat_settings(bio=...)\`.\n` +
          `  8. Track work via tasks. BEFORE non-trivial work — \`tasks_upsert(title=..., status="todo", chat_id=<chat>)\`. ON finish — \`tasks_upsert(id=..., status="done")\`. WHEN delegating — \`tasks_upsert(assignee="<handle>", chat_id=<chat>)\` BEFORE @-mentioning. ON entering a chat — \`tasks_list\`. Silent work is invisible work.`,
      }],
    };
  }
  if (name === "agent_logout") {
    const ownerErr = requireOwner();
    if (ownerErr) return { content: [{ type: "text", text: `✗ ${ownerErr}` }], isError: true };
    logoutAgent();
    return { content: [{ type: "text", text: "✓ Agent identity cleared. Owner session is still active — paste another agent prompt or call agent_login(token)." }] };
  }

  if (name === "add_chat_member") {
    const chat_id = args?.chat_id;
    const agent_id = coerceAgentId(args?.agent_id);
    if (typeof chat_id !== "string" || agent_id === null) {
      return {
        content: [{ type: "text", text: "✗ Required: chat_id (string), agent_id (integer or string-integer)." }],
        isError: true,
      };
    }
    const result = await addChatMember(chat_id, agent_id);
    return {
      content: [
        { type: "text", text: `✓ Member added:\n${JSON.stringify(result, null, 2)}` },
      ],
    };
  }

  if (name === "remove_chat_member") {
    const chat_id = args?.chat_id;
    const agent_id = coerceAgentId(args?.agent_id);
    if (typeof chat_id !== "string" || agent_id === null) {
      return {
        content: [{ type: "text", text: "✗ Required: chat_id (string), agent_id (integer or string-integer)." }],
        isError: true,
      };
    }
    const result = await removeChatMember(chat_id, agent_id);
    return {
      content: [
        { type: "text", text: `✓ Member removed:\n${JSON.stringify(result, null, 2)}` },
      ],
    };
  }

  if (name === "spawn_agent") {
    const chat_id = args?.chat_id;
    const display_name = args?.display_name;
    const bio = args?.bio;
    const auto_launch = args?.auto_launch === true;
    const target = (args?.target ?? "claude") as "claude" | "codex";
    const work_dir = args?.work_dir;

    if (typeof chat_id !== "string" || typeof display_name !== "string") {
      return {
        content: [{
          type: "text",
          text: "✗ Required: chat_id (string), display_name (string). bio is optional.",
        }],
        isError: true,
      };
    }
    if (bio !== undefined && typeof bio !== "string") {
      return {
        content: [{ type: "text", text: "✗ bio must be a string if provided." }],
        isError: true,
      };
    }
    if (target !== "claude" && target !== "codex") {
      return {
        content: [{ type: "text", text: "✗ target must be \"claude\" or \"codex\"." }],
        isError: true,
      };
    }
    if (auto_launch && (typeof work_dir !== "string" || !work_dir.trim())) {
      return {
        content: [{
          type: "text",
          text: "✗ work_dir (string) is required when auto_launch=true.",
        }],
        isError: true,
      };
    }

    const result: any = await spawnAgentInChat(
      chat_id,
      display_name,
      bio as string | undefined,
    );
    const token: string | undefined = result?.auth_token;
    const newAgentDisplayName: string = result?.display_name || display_name;
    const onboarding_prompt = token
      ? `You're being added to an InBetween chat as @${newAgentDisplayName}. ` +
        `Call inbetween.agent_login with auth_token=${token} and you'll be online ` +
        `in the chat. After login, the server pushes your role, members, and any private ` +
        `playbook automatically — read those, then go quiet until @-mentioned.`
      : null;

    // Attempt auto-launch only for target=claude. Codex falls through to the
    // manual-paste output below; the prompt is still useful, the human just
    // opens the Codex window themselves.
    let launched = false;
    let launchInfo: string | null = null;
    if (auto_launch && target === "claude" && onboarding_prompt) {
      const lr = launchClaudeInNewWindow(work_dir as string, onboarding_prompt);
      if (lr.ok) {
        launched = true;
        launchInfo = `✓ New Claude Code window opened in ${work_dir} (platform=${lr.platform}). The agent should come online within a few seconds.`;
      } else {
        launchInfo = `⚠ auto_launch failed on ${lr.platform}: ${lr.error}. Falling back to manual paste — give the prompt below to the human.`;
      }
    } else if (auto_launch && target === "codex") {
      launchInfo = "ℹ codex auto-launch is not implemented yet. Give the prompt below to the human; they paste it into a fresh Codex window manually.";
    }

    const body =
      `✓ Agent spawned:\n${JSON.stringify(result, null, 2)}\n` +
      (launchInfo ? `\n${launchInfo}\n` : "") +
      (onboarding_prompt
        ? launched
          ? "" // window already has the prompt; no need to repeat it for human
          : `\n--- ONBOARDING PROMPT (paste into a fresh IDE) ---\n${onboarding_prompt}`
        : "\n(no auth_token returned — backend may be misconfigured)");

    return { content: [{ type: "text", text: body }] };
  }

  // === Privacy / DND ===
  // === Mute ===
  // === Friend-request ===
  // ===== V0.2 handlers =====
  if (name === "tasks_list") {
    const r = await listTasks(args?.status as string | undefined, (args?.limit as number) || 50);
    return { content: [{ type: "text", text: JSON.stringify((r as any).tasks, null, 2) }] };
  }
if (name === "tasks_upsert") {
    const id = args?.id as number | undefined;
    const payload: any = {};
    for (const k of ["title", "description", "status", "due", "agent_name", "assignee", "chat_id", "completion_note"]) {
      if (args?.[k] !== undefined) payload[k] = args[k];
    }
    if (id != null) {
      const r = await updateTask(id, payload);
      return { content: [{ type: "text", text: `✓ Task #${id} updated
${JSON.stringify(r, null, 2)}` }] };
    }
    if (!payload.title) {
      return { content: [{ type: "text", text: "✗ title required when creating a new task" }], isError: true };
    }
    const r = await createTask(payload);
    return { content: [{ type: "text", text: `✓ Task #${(r as any).id} created
${JSON.stringify(r, null, 2)}` }] };
  }

  // ===== Checkpoints =====
  if (name === "checkpoint_create") {
    const chat_id = args?.chat_id as number | undefined;
    const title = args?.title as string | undefined;
    const summary = args?.summary as string | undefined;
    if (chat_id == null || !title || !summary) {
      return { content: [{ type: "text", text: "✗ chat_id, title, summary all required" }], isError: true };
    }
    const r = await createCheckpoint({ chat_id, title, summary });
    return { content: [{ type: "text", text: `✓ Checkpoint #${(r as any).id} created
${JSON.stringify(r, null, 2)}` }] };
  }
  if (name === "checkpoints_list") {
    const chat_id = args?.chat_id as number | undefined;
    if (chat_id == null) {
      return { content: [{ type: "text", text: "✗ chat_id required" }], isError: true };
    }
    const limit = (args?.limit as number) || 50;
    const r = await listCheckpoints(chat_id, limit);
    return { content: [{ type: "text", text: JSON.stringify((r as any).checkpoints, null, 2) }] };
  }

  // ===== Scheduled pings =====
  if (name === "schedule_ping") {
    const chat_id = args?.chat_id as number | undefined;
    const delay_seconds = args?.delay_seconds as number | undefined;
    const content = args?.content as string | undefined;
    if (chat_id == null || delay_seconds == null || !content) {
      return { content: [{ type: "text", text: "✗ chat_id, delay_seconds, content all required" }], isError: true };
    }
    const r = await createScheduledPing({ chat_id, delay_seconds, content });
    return { content: [{ type: "text", text: `✓ Ping #${(r as any).id} scheduled — fires at ${(r as any).run_at}
${JSON.stringify(r, null, 2)}` }] };
  }
  if (name === "list_scheduled_pings") {
    const chat_id = args?.chat_id as number | undefined;
    if (chat_id == null) {
      return { content: [{ type: "text", text: "✗ chat_id required" }], isError: true };
    }
    const r = await listScheduledPings(chat_id);
    return { content: [{ type: "text", text: JSON.stringify((r as any).pings, null, 2) }] };
  }
  if (name === "cancel_scheduled_ping") {
    const id = args?.id as number | undefined;
    if (id == null) {
      return { content: [{ type: "text", text: "✗ id required" }], isError: true };
    }
    await cancelScheduledPing(id);
    return { content: [{ type: "text", text: `✓ Ping #${id} cancelled` }] };
  }

  // ===== Unified chats =====
  if (name === "list_chats") {
    const r = await listChats();
    return { content: [{ type: "text", text: JSON.stringify((r as any).chats, null, 2) }] };
  }
  if (name === "chat_messages") {
    const r: any = await chatMessages({
      chat_id: args!.chat_id as string,
      limit: args?.limit as number | undefined,
      before: args?.before as string | undefined,
      since: args?.since as string | undefined,
      until: args?.until as string | undefined,
      unread: args?.unread as boolean | undefined,
    });
    // Header: каждый раз когда агент читает чат, мы напоминаем ему:
    //   1. кто здесь и чем занимается (member bio + notify rules);
    //   2. его собственные инструкции (instructions, видны только ему).
    const blocks: string[] = [];
    if (Array.isArray(r.members) && r.members.length > 0) {
      const lines = r.members.map((m: any) => {
        const notifyTag = m.notify_mode === "always"
          ? "always pinged"
          : m.notify_mode === "on_mention"
            ? `pinged ONLY when content contains @${m.name} — tag explicitly to wake them`
            : "never pinged (silent reader)";
        const bioTag = m.bio ? ` · bio: ${m.bio}` : "";
        return `  @${m.name} — ${notifyTag}${bioTag}`;
      });
      blocks.push(`=== MEMBERS (chat ${r.chat_id}) ===\n${lines.join("\n")}`);
    }
    if (r.instructions) {
      blocks.push(`=== MY INSTRUCTIONS (chat ${r.chat_id}, only I see this) ===\n${r.instructions}`);
    }
    const header = blocks.length ? blocks.join("\n\n") + "\n=== END ===\n\n" : "";
    const body = JSON.stringify(r.messages, null, 2);
    return { content: [{ type: "text", text: header + body }] };
  }
  if (name === "chat_mark_read") {
    await chatMarkRead(args!.chat_id as string);
    return { content: [{ type: "text", text: `✓ Chat ${args!.chat_id} marked read` }] };
  }
  if (name === "chat_send") {
    const r: any = await chatSend(
      args!.chat_id as string,
      args!.content as string,
      (args!.attachments as any[]) || [],
      {},
      typeof args?.reply_to_message_id === "string" ? args.reply_to_message_id : undefined,
    );
    return {
      content: [
        {
          type: "text",
          text: `✓ Sent to chat ${args!.chat_id}. Recipients: ${r.recipients}, delivered: ${r.delivered_to}. ID: ${r.message_id}`,
        },
      ],
    };
  }
  if (name === "chat_thread") {
    const chat_id = args?.chat_id;
    const root = args?.root_message_id;
    if (typeof chat_id !== "string" || typeof root !== "string") {
      return {
        content: [{ type: "text", text: "✗ Required: chat_id (string), root_message_id (UUID string)." }],
        isError: true,
      };
    }
    const r: any = await chatThread(chat_id, root);
    return { content: [{ type: "text", text: JSON.stringify(r.messages, null, 2) }] };
  }
  if (name === "attachment_download") {
    const messageId = args!.message_id as string;
    const index = (args?.index as number | undefined) ?? 0;
    // Find the message in the local inbox cache (populated by WS pushes
    // and the boot inbox_summary). Fall back to a backend round-trip if
    // it's not cached locally.
    let msg = inbox.find((m) => m.message_id === messageId);
    if (!msg) {
      // Pull recent inbox slice fresh from backend.
      try {
        const fresh = await api<any>("GET", "/inbox?limit=200");
        const items: any[] = fresh?.messages || [];
        msg = items.find((m: any) => m.message_id === messageId);
      } catch (_) { /* fall through */ }
    }
    if (!msg) {
      return { content: [{ type: "text", text: `✗ message_id=${messageId} not found in your visible inbox` }], isError: true };
    }
    const atts = Array.isArray(msg.attachments) ? msg.attachments : [];
    const att = atts[index];
    if (!att) {
      return { content: [{ type: "text", text: `✗ attachment index=${index} not found (message has ${atts.length} attachment(s))` }], isError: true };
    }
    const path = att.path as string;
    const bucket = (att.bucket as string) || "chat-attachments";
    if (!path) {
      return { content: [{ type: "text", text: `✗ attachment object has no \`path\` — looks malformed: ${JSON.stringify(att)}` }], isError: true };
    }
    let signed: any;
    try {
      signed = await api<any>("POST", "/attachments/download-url", { path, bucket });
    } catch (e: any) {
      return { content: [{ type: "text", text: `✗ download-url request failed: ${e?.message || e}` }], isError: true };
    }
    return {
      content: [{
        type: "text",
        text: JSON.stringify({
          name: att.name ?? null,
          size: att.size ?? null,
          mime: att.mime ?? att.content_type ?? null,
          download_url: signed.url,
          expires_in_seconds: 600,
          hint: "Use WebFetch (or any HTTP client) on download_url within 10 minutes. The URL is single-use-friendly but you can call attachment_download again to refresh.",
        }, null, 2),
      }],
    };
  }
  if (name === "attachment_send") {
    const chatId = args!.chat_id as string;
    const content = args!.content as string;
    const localPath = args!.local_path as string;
    const overrideName = args?.name as string | undefined;
    // Lazy import to avoid module-level overhead.
    const fs = await import("fs");
    const pathLib = await import("path");
    let stat: any;
    try {
      stat = fs.statSync(localPath);
    } catch (e: any) {
      return { content: [{ type: "text", text: `✗ local_path not readable: ${e?.message || e}` }], isError: true };
    }
    if (!stat.isFile()) {
      return { content: [{ type: "text", text: `✗ local_path is not a regular file` }], isError: true };
    }
    const size = stat.size;
    if (size > 25 * 1024 * 1024) {
      return { content: [{ type: "text", text: `✗ file too large (${size} bytes); cap is 25MB` }], isError: true };
    }
    const fileName = overrideName || pathLib.basename(localPath);
    const ext = pathLib.extname(fileName).toLowerCase();
    const mimeMap: Record<string, string> = {
      ".png": "image/png",
      ".jpg": "image/jpeg",
      ".jpeg": "image/jpeg",
      ".webp": "image/webp",
      ".gif": "image/gif",
      ".pdf": "application/pdf",
      ".txt": "text/plain",
      ".log": "text/plain",
      ".md": "text/markdown",
      ".json": "application/json",
    };
    const mime = mimeMap[ext] || "application/octet-stream";
    // 1. Sign upload — returns { upload_url, path, bucket, ... }
    let sig: any;
    try {
      sig = await api<any>("POST", "/attachments/sign", {
        filename: fileName,
        content_type: mime,
        size,
        chat_id: Number(chatId),
      });
    } catch (e: any) {
      return { content: [{ type: "text", text: `✗ sign failed: ${e?.message || e}` }], isError: true };
    }
    // 2. PUT bytes to signed URL.
    const buf = fs.readFileSync(localPath);
    const putRes = await fetch(sig.upload_url, {
      method: "PUT",
      headers: { "Content-Type": mime },
      body: buf,
    });
    if (!putRes.ok) {
      return { content: [{ type: "text", text: `✗ upload PUT failed: ${putRes.status} ${await putRes.text()}` }], isError: true };
    }
    // 3. chat_send with attachment.
    const att = { path: sig.path, bucket: sig.bucket, mime, size, name: fileName };
    let send: any;
    try {
      send = await chatSend(chatId, content, [att], {});
    } catch (e: any) {
      return { content: [{ type: "text", text: `✗ uploaded ok but chat_send failed: ${e?.message || e}` }], isError: true };
    }
    return {
      content: [{
        type: "text",
        text: `✓ Uploaded ${fileName} (${Math.round(size / 1024)} KB, ${mime}) and sent to chat ${chatId}. Recipients: ${send.recipients}, delivered: ${send.delivered_to}. message_id: ${send.message_id}`,
      }],
    };
  }
  if (name === "search_messages") {
    const r = await searchMessages({
      q: args!.q as string,
      limit: args?.limit as number | undefined,
      since: args?.since as string | undefined,
      until: args?.until as string | undefined,
      chat_id: args?.chat_id as string | undefined,
    });
    return { content: [{ type: "text", text: JSON.stringify((r as any).messages, null, 2) }] };
  }
  if (name === "list_agents") {
    const chat_id = args?.chat_id as string;
    if (!chat_id) {
      return { content: [{ type: "text", text: "✗ chat_id required" }], isError: true };
    }
    const chat: any = await api("GET", `/chats/${encodeURIComponent(chat_id)}`);
    const coordId = chat?.coordinator_agent_id ?? null;
    const members = (chat?.members || []).map((m: any) => ({
      agent_id: m.agent_id,
      agent_name: m.agent_name,
      display_name: m.display_name,
      owner_handle: m.owner_handle,
      is_me: !!m.is_me,
      is_coordinator: !!m.is_coordinator,
      notify_mode: m.notify_mode,
      // When non-null, this member is a "linked group" — a channel
      // pointing at another chat. Mention it like any other agent;
      // a direct reply on its reply bubble bounces back automatically.
      // No become_agent / no executor on the other side.
      linked_chat_id: m.linked_chat_id ?? null,
    }));
    return {
      content: [{
        type: "text",
        text: JSON.stringify({
          chat_id: chat?.id ?? chat_id,
          title: chat?.title,
          coordinator_agent_id: coordId,
          members,
        }, null, 2),
      }],
    };
  }

  if (name === "get_chat") {
    const r = await getChat(args!.chat_id as string);
    return { content: [{ type: "text", text: JSON.stringify(r, null, 2) }] };
  }
  if (name === "set_chat_settings") {
    // agent_id is optional — coerce both number and string-integer; reject
    // a present-but-unparseable value with a clear message so the LLM can
    // self-correct instead of silently editing itself.
    let targetAgentId: number | undefined;
    if (args?.agent_id !== undefined && args?.agent_id !== null) {
      const coerced = coerceAgentId(args.agent_id);
      if (coerced === null) {
        return {
          content: [{ type: "text", text: "✗ agent_id must be an integer or string-integer." }],
          isError: true,
        };
      }
      targetAgentId = coerced;
    }
    await setChatSettings({
      chat_id: args!.chat_id as string,
      notify_mode: args?.notify_mode as any,
      bio: args?.bio as string | undefined,
      instructions: args?.instructions as string | undefined,
      agent_id: targetAgentId,
    });
    // Local instant refresh: backend will ALSO push `chat_member_settings_updated`
    // via WS, but that has network round-trip latency. Only refresh locally when
    // the write targeted ourselves — if a coordinator just rewrote a teammate's
    // row, the teammate's MCP refreshes via the WS event; our own context block
    // is unchanged.
    const editedSelf = targetAgentId === undefined;
    if (editedSelf && (args?.bio !== undefined || args?.instructions !== undefined)) {
      refreshChatContext().catch((e) =>
        console.error(`[inbetween] post-set_chat_settings context refresh threw: ${e?.message || e}`),
      );
    }
    const who = editedSelf ? "yourself" : `agent #${targetAgentId}`;
    return { content: [{ type: "text", text: `✓ Settings saved for ${who} in chat ${args!.chat_id}` }] };
  }
  if (name === "inbox_unread") {
    const r = await inboxUnread({
      limit: args?.limit as number | undefined,
      since: args?.since as string | undefined,
    });
    return { content: [{ type: "text", text: JSON.stringify((r as any).messages, null, 2) }] };
  }

  throw new Error(`Unknown tool: ${name}`);
});

// === RESOURCES ===
const subscribedUris = new Set<string>();

server.setRequestHandler(ListResourcesRequestSchema, async () => ({
  resources: [
    {
      uri: "inbetween://inbox",
      name: "InBetween Inbox",
      description: "All messages received by the active agent. Check this for incoming agent messages.",
      mimeType: "application/json",
    },
    {
      uri: "inbetween://profile",
      name: "My InBetween Profile",
      description: "Profile of the active agent — name, presence, pending message count.",
      mimeType: "application/json",
    },
    {
      uri: "inbetween://tasks",
      name: "InBetween Tasks",
      description: "Open and in-progress tasks for the active agent.",
      mimeType: "application/json",
    },
  ],
}));

// Zod schemas — SDK требует .shape.method literal, нельзя передавать плоский объект.
const SubscribeSchema = z.object({
  method: z.literal("resources/subscribe"),
  params: z.object({ uri: z.string() }).passthrough().optional(),
});
const UnsubscribeSchema = z.object({
  method: z.literal("resources/unsubscribe"),
  params: z.object({ uri: z.string() }).passthrough().optional(),
});
const ChannelPermissionNotificationSchema = z.object({
  method: z.literal("notifications/claude/channel/permission"),
  params: z.record(z.unknown()).optional(),
});

// Subscribe handler — Claude Code calls this to subscribe к resource updates.
// `as any` cast на schema — TS overload resolution для setRequestHandler
// уходит в TS2589 ("excessively deep") при росте числа handler'ов в файле.
// Zod продолжает runtime-валидировать, типобезопасность handler'а сохраняется.
server.setRequestHandler(SubscribeSchema as any, async (request: any) => {
  const uri = request.params?.uri;
  if (uri) {
    subscribedUris.add(uri);
    console.error(`[inbetween] Subscribed to ${uri}`);
  }
  return {};
});

server.setRequestHandler(UnsubscribeSchema as any, async (request: any) => {
  const uri = request.params?.uri;
  if (uri) {
    subscribedUris.delete(uri);
  }
  return {};
});

// Claude Code Channels: permission notification handler.
// Claude Code посылает notifications/claude/channel/permission когда юзер
// одобрил/отклонил наш канал. Нам этот ack просто проглотить — но handler
// должен быть зарегистрирован, иначе Claude Code считает server невалидным.
server.setNotificationHandler(
  ChannelPermissionNotificationSchema as any,
  async (notification: any) => {
    const params: any = notification.params || {};
    console.error(
      `[inbetween] channel permission: request_id=${params.request_id} behavior=${params.behavior}`
    );
  }
);

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const { uri } = request.params;

  if (uri === "inbetween://inbox") {
    const result = await fetchInbox();
    return {
      contents: [
        {
          uri,
          mimeType: "application/json",
          text: JSON.stringify(result.messages, null, 2),
        },
      ],
    };
  }

  if (uri === "inbetween://profile") {
    // /agents/whoami даёт полный self-профиль (с owner_id, флагами).
    const [whoamiRes, inboxRes] = await Promise.all([
      api<any>("GET", "/agents/whoami").catch(() => null),
      fetchInbox(true).catch(() => ({ messages: [] })),
    ]);
    const profile = whoamiRes || { display_name: activeAgentName ?? "(unknown)", name: null };
    const pending = (inboxRes as any).messages || [];
    // Self-identity preamble — агент должен чётко понимать кто он и кто его
    // owner. Помещаем вверху JSON чтобы LLM видела это первым.
    const visibleName = profile.display_name || profile.name || "(unknown)";
    const ownerLabel = profile.owner_handle ? `@${profile.owner_handle}` : "(unnamed owner)";
    const identity = {
      i_am: `@${visibleName}`,
      my_owner: ownerLabel,
      note:
        "When a message has from_human=true, it is from " + ownerLabel + " (the OWNER, a real person) " +
        "— not from another agent. When from_human=false, it is from another agent. " +
        "You speak as @" + visibleName + " — never claim to be the owner.",
    };
    return {
      contents: [
        {
          uri,
          mimeType: "application/json",
          text: JSON.stringify(
            {
              identity,
              ...profile,
              pending_messages_count: pending.length,
              pending_preview: pending.slice(0, 3).map((m: any) => ({
                from: m.from_agent,
                from_human: !!(m.metadata && (m.metadata as any).from_human),
                preview: (m.content || "").slice(0, 100),
                sent_at: m.sent_at,
              })),
            },
            null,
            2
          ),
        },
      ],
    };
  }

  if (uri === "inbetween://tasks") {
    const todo = await listTasks("todo").catch(() => ({ tasks: [] }));
    const inProgress = await listTasks("in_progress").catch(() => ({ tasks: [] }));
    return {
      contents: [{
        uri,
        mimeType: "application/json",
        text: JSON.stringify({
          todo: (todo as any).tasks,
          in_progress: (inProgress as any).tasks,
        }, null, 2),
      }],
    };
  }

  throw new Error(`Unknown resource: ${uri}`);
});

// =================================================================
// MAIN
// =================================================================
async function main() {
  if (activeAgentName) {
    console.error(`[inbetween] Starting MCP server (restored session @${activeAgentName})`);
  } else {
    console.error("[inbetween] Starting MCP server — waiting for owner_login + agent_login");
  }

  // Best-effort npm update notification (24h cached, fire-and-forget so we
  // don't slow startup). Reads our own package.json for the running version.
  (() => {
    try {
      const r = createRequire(import.meta.url);
      const current = r("../package.json").version as string;
      maybeNotifyUpdate(current).catch(() => {});
    } catch {}
  })();

  // Defer WS + polling до тех пор, пока CC не пришлёт `notifications/initialized`.
  // Иначе pending messages с backend (приходят сразу после WS open) эмитятся через
  // notifications/claude/channel ДО того, как у CC завершён init handshake — и
  // CC их тихо дропает. Real-time messages приходят позже, когда init уже
  // завершён, поэтому работают.
  let networkStarted = false;
  const startNetwork = () => {
    if (networkStarted) return;
    networkStarted = true;
    if (!activeAgentToken) {
      console.error("[inbetween] MCP initialized — idle (call agent_login to enable WS push)");
      return;
    }
    console.error("[inbetween] MCP initialized — connecting WS + polling");
    startPolling();
    connectWebSocket();
  };
  server.oninitialized = startNetwork;

  // Start MCP via stdio
  const transport = new StdioServerTransport();
  await server.connect(transport);

  // Safety net: если по какой-то причине CC не пришлёт `initialized`
  // (старый клиент, кастомный harness и т.д.) — стартуем сами через 5s.
  setTimeout(startNetwork, 5000);

  console.error("[inbetween] MCP server ready");
}

main().catch((err) => {
  console.error("[inbetween] Fatal error:", err);
  process.exit(1);
});
