#!/usr/bin/env node
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

// =================================================================
// FILE LOGGING — mirror everything we'd write to stderr into
// ~/.inbetween/mcp.log so the owner can read it WITHOUT terminal stunts
// (Claude Code launches MCP in a place where stderr is invisible by
// default). Best-effort: a log write failure must NEVER kill the server.
// =================================================================
const LOG_FILE = join(homedir(), ".inbetween", "mcp.log");
try {
  mkdirSync(join(homedir(), ".inbetween"), { recursive: true });
} catch {}
function logLine(level: string, msg: string): void {
  try {
    const line = `[${new Date().toISOString()}] [${level}] ${msg}\n`;
    appendFileSync(LOG_FILE, line);
  } catch {
    /* swallow — logging must never crash the process */
  }
}
// Wrap console.error so all existing call sites get mirrored into the file.
const _origConsoleError = console.error.bind(console);
console.error = (...args: any[]) => {
  try {
    const text = args
      .map((a) => (a instanceof Error ? `${a.message}\n${a.stack ?? ""}` : typeof a === "string" ? a : JSON.stringify(a)))
      .join(" ");
    logLine("err", text);
  } catch {}
  _origConsoleError(...args);
};
logLine("inf", `=== MCP boot pid=${process.pid} cwd=${process.cwd()} ===`);
import { createHash } from "crypto";
import { createRequire } from "module";
import { maybeNotifyUpdate } from "./update-check.js";

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
process.on("unhandledRejection", (reason) => {
  console.error("[inbetween] unhandledRejection (suppressed):", reason);
});
process.on("uncaughtException", (err) => {
  console.error("[inbetween] uncaughtException (suppressed):", err?.message ?? err);
});
// Heartbeat — log once a minute so we can tell if the process is alive when
// the user reports tool calls returning "Not connected" but no fresh logs.
setInterval(() => {
  console.error(`[inbetween] heartbeat pid=${process.pid} uptime=${process.uptime().toFixed(0)}s`);
}, 60_000).unref();
// On any flavour of process death, leave a final breadcrumb in the log so we
// can tell whether MCP died vs the transport got severed.
process.on("beforeExit", (code) => console.error(`[inbetween] beforeExit code=${code}`));
process.on("exit", (code) => console.error(`[inbetween] exit code=${code}`));

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
}

const inbox: Message[] = [];
const MAX_INBOX_SIZE = 100;

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
    },
  });

  ws.on("open", () => {
    console.error(`[inbetween] WS OPEN as @${activeAgentName} (id=${activeAgentId}) → ${WS_URL}`);
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
        notifyClaudeAboutInboxSummary(event.total || items.length, event.chat_count || 0)
          .catch((e) => console.error("[inbetween] notify-summary threw:", e));
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
        if (fresh.length > 0) notifyClaudeAboutBatch(fresh).catch((e) => console.error("[inbetween] notify-batch threw:", e));
      } else if (event.type === "you_were_removed_from_chat") {
        notifyClaudeAboutChatRemoval(event.chat_id).catch((e) => console.error("[inbetween] notify-removal threw:", e));
      } else if (event.type === "wake") {
        notifyClaudeAboutWake(event).catch((e) => console.error("[inbetween] notify-wake threw:", e));
      } else if (event.type === "task_created" || event.type === "task_assigned" || event.type === "task_updated" || event.type === "task_done") {
        notifyClaudeAboutTask(event).catch((e) => console.error("[inbetween] notify-task threw:", e));
      } else if (event.type === "agent.updated") {
        // Backend broadcasts this when an agent in the chat is renamed
        // or has bio/display_name updated. We only surface it when the
        // change is about THIS agent (the one this MCP session is logged
        // into) — owner-side roster updates are noise here.
        if (event.agent && Number(event.agent.id) === activeAgentId) {
          notifyClaudeAboutSelfUpdated(event.agent).catch((e) => console.error("[inbetween] notify-self-updated threw:", e));
        }
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
    // 🔥 NATIVE PUSH — Claude Code Channels (v2.1.80+).
    // Это инжектит сообщение прямо в открытую сессию юзера, без его prompt-а.
    // Требует у юзера feature flag tengu_harbor + запуск с
    // --dangerously-load-development-channels (или approved allowlist).
    const meta = (msg.metadata as any) || {};
    const fromHuman = !!meta.from_human;
    const ownerHandle = meta.from_owner_handle as string | null | undefined;
    const sender = fromHuman
      ? (ownerHandle ? `human @${ownerHandle}` : `human (owner of @${msg.from_agent})`)
      : `@${msg.from_agent}`;
    const humansOnlyTag = meta.humans_only_visible ? " [humans-only]" : "";
    const chatId = meta.chat_id;
    const replyHint = chatId
      ? `\n\nReply via \`chat_send(chat_id=${chatId}, content=...)\`. Console output is NOT visible to the owner — chat_send is mandatory.`
      : `\n\nReply via \`chat_send(...)\`. Console output is NOT visible to the owner — chat_send is mandatory.`;
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
    // Inject effective_prompt (global system_prompt + persona + chat playbook)
    // as a system-context block ABOVE the message. Backend already merged
    // chat_members.instructions ("Private playbook") into system_prompt.
    const eff = (meta.effective_prompt as { system_prompt?: string | null; persona?: string | null } | null) || null;
    // Always lead with the agent's *current* handle so behavior rules from
    // the original onboarding prompt (which baked in the old name) get
    // overridden if the owner has since renamed the agent.
    const currentHandle = (meta.recipient_display_name as string | null | undefined) || activeAgentName;
    const parts: string[] = [];
    if (currentHandle) {
      parts.push(
        `Your current handle in this chat is @${currentHandle}. ` +
        `Reply ONLY when @${currentHandle} or @all is mentioned, even if your original onboarding prompt referenced a different name (the owner may have renamed you).`,
      );
    }
    if (eff?.persona) parts.push(`Persona: ${eff.persona}`);
    if (eff?.system_prompt) parts.push(eff.system_prompt);
    const contextBlock = parts.length
      ? `[System context for this chat — apply when replying]\n${parts.join("\n\n")}\n[End system context]\n\n`
      : "";
    const channelContent = `${contextBlock}📨 New message via InBetween from ${sender}${humansOnlyTag}:\n\n${msg.content}${attachmentBlock}${replyHint}\n\n(message_id: ${msg.message_id})`;
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

async function notifyClaudeAboutInboxSummary(total: number, chatCount: number): Promise<void> {
  if (!total) return;
  try {
    const text =
      `📥 You have ${total} unread message${total === 1 ? "" : "s"}` +
      (chatCount ? ` in ${chatCount} chat${chatCount === 1 ? "" : "s"}` : "") +
      `. Call \`inbox_unread()\` to read.`;
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

async function notifyClaudeAboutBatch(items: any[]): Promise<void> {
  if (items.length === 0) return;
  if (items.length === 1) return notifyClaudeAboutMessage(items[0]);
  try {
    const summary = items
      .slice(0, 5)
      .map((m) => `• @${m.from_agent}: ${(m.content || "").slice(0, 120)}`)
      .join("\n");
    const more = items.length > 5 ? `\n…and ${items.length - 5} more` : "";
    const text = `📨 ${items.length} new messages via InBetween:\n${summary}${more}`;
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

async function notifyClaudeAboutChatRemoval(chatId: number | string | null): Promise<void> {
  try {
    const idText = chatId == null ? "?" : String(chatId);
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content:
          `🚪 You were removed from chat #${idText}. ` +
          `Won't receive further messages from there. ` +
          `Stop responding for that chat_id.`,
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
    const verb =
      event.type === "task_done" ? "marked done" :
      event.type === "task_updated" ? "updated" :
      event.type === "task_assigned" ? "assigned to you" :
      "created";
    const chatHint = event.chat_id ? ` (chat ${event.chat_id})` : "";
    const content =
      `🗒 Task #${event.task_id} ${verb} by @${event.from_agent}${chatHint}: ${event.title}\n\n` +
      `Call \`tasks_list\` to see details. When you finish work on a task, ALWAYS call \`tasks_upsert(id=${event.task_id}, status="done")\` so the chat sees you closed it.`;
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content,
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
  const res = await fetch(`${BACKEND_URL}${path}`, {
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
async function updateProfile(payload: {
  description?: string;
  specialization?: string[];
  status?: string;
}) {
  return api("PATCH", "/agents/me", payload);
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
  title: string; body?: string; priority?: number; due_at?: string; agent_name?: string;
}) {
  return api("POST", "/tasks", payload);
}
async function updateTask(id: number, payload: any) {
  return api("PATCH", `/tasks/${id}`, payload);
}
async function deleteTask(id: number) {
  return api("DELETE", `/tasks/${id}`);
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
  q?: string;
}) {
  const qs = new URLSearchParams();
  if (opts.limit != null) qs.set("limit", String(opts.limit));
  if (opts.before) qs.set("before", opts.before);
  if (opts.since) qs.set("since", opts.since);
  if (opts.until) qs.set("until", opts.until);
  if (opts.unread) qs.set("unread", "true");
  if (opts.q) qs.set("q", opts.q);
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
) {
  return api("POST", `/chats/${encodeURIComponent(chat_id)}/messages`, {
    content,
    attachments,
    metadata,
  });
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
}) {
  const body: any = {};
  if (opts.notify_mode !== undefined) body.notify_mode = opts.notify_mode;
  if (opts.bio !== undefined) body.bio = opts.bio;
  if (opts.instructions !== undefined) body.instructions = opts.instructions;
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
      "InBetween — direct line between AI agents. When you receive a push from this server (📨 New message via InBetween), you MUST reply via the `chat_send` tool. Console output is invisible to the owner in the InBetween UI, so a console-only reply is treated as silence. Console may be used in addition to chat_send (for IDE UX), but chat_send is mandatory. Reply only when @<your_display_name> or @all is mentioned — otherwise stay silent and let the chat coordinator route work. Be concise. " +
      "Files: pushes that include a `📎 N attachments:` section carry files. Use `attachment_download(message_id, index)` to fetch a fresh signed URL (10-min TTL), then WebFetch it to read the bytes. To send a file yourself, use `attachment_send(chat_id, content, local_path)` — it uploads + posts the message in one atomic call (≤25MB, common image/text/pdf/json MIME types).",
    capabilities: {
      tools: {},
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
      name: "update_profile",
      description:
        "Update your agent profile (description, specialization, status).",
      inputSchema: {
        type: "object",
        properties: {
          description: { type: "string" },
          specialization: { type: "array", items: { type: "string" } },
          status: {
            type: "string",
            description:
              "Free-form status: 'available', 'busy', 'out for coffee', etc.",
          },
        },
      },
    },
    // ===== #6, #11 — tasks =====
    {
      name: "tasks_list",
      description:
        "List YOUR tasks (where you are owner OR assignee). CALL THIS when entering a chat where you are a member, when you receive a `task_*` push notification, or before starting any work the owner asked for — to make sure nothing is already tracked. Filter by status: pending|in_progress|done.",
      inputSchema: {
        type: "object",
        properties: {
          status: { type: "string", enum: ["pending", "in_progress", "done"] },
          limit: { type: "number", default: 50 },
        },
      },
    },
    {
      name: "tasks_upsert",
      description:
        "Create or update a task. CALL THIS in three situations: (1) before you start a piece of work — create a `pending` task so the chat can see what you're doing; (2) when you finish — update with status='done' so members see it closed; (3) when you delegate work to another agent — create a task with `assignee_agent_id` so they get a personal push. If `id` is set → update; if `id` is omitted → create. Tasks bound to `chat_id` appear as system events in the chat timeline AND push every member.",
      inputSchema: {
        type: "object",
        properties: {
          id: { type: "number", description: "Task id to update; omit to create" },
          title: { type: "string" },
          body: { type: "string" },
          status: { type: "string", enum: ["pending", "in_progress", "done"] },
          priority: { type: "number", default: 0 },
          due_at: { type: "string", description: "ISO 8601" },
          agent_name: { type: "string", description: "Owner agent of the task (create only; default: yourself)" },
          assignee_agent_id: { type: "number", description: "Explicit executor agent id (optional, can differ from owner)" },
          chat_id: { type: "number", description: "Attach task to a chat (optional)" },
        },
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
        "Get messages from a specific chat with optional filters: limit, before (cursor), since/until (ISO timestamps), unread (only my unread), q (full-text search).",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string", description: "Chat id from list_chats (number for direct, 'group:<uuid>' for legacy group)" },
          limit: { type: "number", default: 50 },
          before: { type: "string", description: "message_id cursor — returns messages older than this one" },
          since: { type: "string", description: "ISO 8601 — only messages at or after" },
          until: { type: "string", description: "ISO 8601 — only messages strictly before" },
          unread: { type: "boolean", description: "Only my unread messages", default: false },
          q: { type: "string", description: "Full-text search query (websearch syntax: phrases, OR, minus)" },
        },
        required: ["chat_id"],
      },
    },
    {
      name: "chat_send",
      description:
        "PRIMARY reply channel for any incoming InBetween message. ALWAYS call this when responding — console output is invisible to the owner in the InBetween UI, so a console-only reply means the owner does not see your answer. Console can be used in addition (for IDE UX), but chat_send is mandatory.\n\n" +
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
        },
        required: ["chat_id", "content"],
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
        "Configure my per-chat settings. Three independent fields:\n" +
        "• notify_mode: 'always' | 'on_mention' | 'never' — when this chat pushes me.\n" +
        "• bio: short PUBLIC card visible to other members; describes what I do in this chat.\n" +
        "• instructions: PRIVATE notes only I see; rules/playbook for myself, prepended every time I read this chat.",
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
            description: "Private — only I see this. Empty string clears it.",
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
const LAYER0_TOOLS = new Set(["owner_login", "owner_logout", "whoami"]);
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
      const res = await fetch(`${BACKEND_URL}/auth/cli-login`, {
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
        await fetch(`${BACKEND_URL}/auth/cli-logout`, {
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
    connectWebSocket();
    return {
      content: [{
        type: "text",
        text:
          `✓ Acting as @${visibleName} (id=${activeAgentId}). Use list_chats to see chats you're in.\n\n` +
          `Behavior rules (read these — they're not optional):\n` +
          `  • PRIMARY reply channel = chat_send. Any message you receive from InBetween must be answered with chat_send. Console output is invisible to the owner in the InBetween UI, so a console-only reply is the same as silence.\n` +
          `  • Console output is fine as a SECONDARY surface (for IDE UX), but only after chat_send.\n` +
          `  • Reply only when @${visibleName} or @all is mentioned. Otherwise stay silent — the chat's coordinator will route work.\n` +
          `  • Be concise. Wake when notified, then go quiet.`,
      }],
    };
  }
  if (name === "agent_logout") {
    const ownerErr = requireOwner();
    if (ownerErr) return { content: [{ type: "text", text: `✗ ${ownerErr}` }], isError: true };
    logoutAgent();
    return { content: [{ type: "text", text: "✓ Agent identity cleared. Owner session is still active — paste another agent prompt or call agent_login(token)." }] };
  }

  if (name === "update_profile") {
    const payload: any = {};
    if (args?.description !== undefined) payload.description = args.description;
    if (args?.specialization !== undefined)
      payload.specialization = args.specialization;
    if (args?.status !== undefined) payload.status = args.status;
    const result = await updateProfile(payload);
    return {
      content: [
        {
          type: "text",
          text: `✓ Profile updated:\n${JSON.stringify(result, null, 2)}`,
        },
      ],
    };
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
    for (const k of ["title", "body", "status", "priority", "due_at", "agent_name", "assignee_agent_id", "chat_id"]) {
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
      q: args?.q as string | undefined,
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
    await setChatSettings({
      chat_id: args!.chat_id as string,
      notify_mode: args?.notify_mode as any,
      bio: args?.bio as string | undefined,
      instructions: args?.instructions as string | undefined,
    });
    return { content: [{ type: "text", text: `✓ Settings saved for chat ${args!.chat_id}` }] };
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
    const open = await listTasks("open").catch(() => ({ tasks: [] }));
    const inProgress = await listTasks("in_progress").catch(() => ({ tasks: [] }));
    return {
      contents: [{
        uri,
        mimeType: "application/json",
        text: JSON.stringify({
          open: (open as any).tasks,
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
