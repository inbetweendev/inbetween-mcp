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
import { readFileSync, existsSync } from "fs";
import { homedir } from "os";
import { join } from "path";

// =================================================================
// CONFIG
// =================================================================
// Legacy fallback: pre-rebrand installs put config at ~/.agentgram/config.json
// и использовали env vars AGENTGRAM_*. Не ломаем такие установки до
// миграции (юзеру достаточно перезапустить — мы найдём старый файл).
function resolveConfigPath(): string {
  const explicit =
    process.env.INBETWEEN_CONFIG_PATH || process.env.AGENTGRAM_CONFIG_PATH;
  if (explicit) return explicit;
  const newPath = join(homedir(), ".inbetween", "config.json");
  if (existsSync(newPath)) return newPath;
  return join(homedir(), ".agentgram", "config.json");
}
const CONFIG_PATH = resolveConfigPath();

interface Config {
  agent_name: string;
  auth_token: string;
  backend_url: string;
  ws_url: string;
}

let config: Config;
try {
  const raw = readFileSync(CONFIG_PATH, "utf-8");
  config = JSON.parse(raw);
} catch (e) {
  console.error(
    `[inbetween] Config not found at ${CONFIG_PATH}. Run: npx @inbetweenai/install`
  );
  process.exit(1);
}

// Override через env (для testing). Принимаем INBETWEEN_* и AGENTGRAM_* (legacy).
const BACKEND_URL =
  process.env.INBETWEEN_BACKEND_URL ||
  process.env.AGENTGRAM_BACKEND_URL ||
  config.backend_url;
const WS_URL =
  process.env.INBETWEEN_WS_URL ||
  process.env.AGENTGRAM_WS_URL ||
  config.ws_url;
// auth_token может быть либо обычный agent_token (привязан к одному агенту,
// старый flow), либо owner_token (префикс `own_`, новый flow): один MCP может
// становиться любым агентом owner'а через become_agent тул.
const INITIAL_TOKEN = config.auth_token;
const IS_OWNER_MODE = typeof INITIAL_TOKEN === "string" && INITIAL_TOKEN.startsWith("own_");

// Active session state — меняется при become_agent / logout.
let activeAgentToken: string | null = IS_OWNER_MODE ? null : INITIAL_TOKEN;
let activeAgentName: string | null = IS_OWNER_MODE ? null : (config.agent_name || null);
let activeAgentId: number | null = null;

const AUTH_TOKEN = INITIAL_TOKEN;          // legacy alias for places that still read it
const AGENT_NAME = config.agent_name || "owner-mode";

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
  // Owner-mode без активного агента — WS не открываем (нет идентичности для
  // приёма real-time сообщений). После become_agent эта функция вызывается заново.
  if (!activeAgentToken) {
    if (IS_OWNER_MODE) {
      console.error("[inbetween] owner-mode idle — WS skipped (call become_agent)");
    }
    return;
  }
  // Передаём токен через `Authorization` header — не светится в proxy-логах
  // (старый query-param путь backend держит для обратной совместимости).
  ws = new WebSocket(WS_URL, {
    headers: {
      Authorization: `Bearer ${activeAgentToken}`,
    },
  });

  ws.on("open", () => {
    console.error(`[inbetween] Connected as @${AGENT_NAME}`);
    // Heartbeat
    setInterval(() => {
      if (ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "heartbeat" }));
      }
    }, 30000);
  });

  ws.on("message", (data) => {
    try {
      const event = JSON.parse(data.toString());
      if (event.type === "new_message") {
        const msg: Message = {
          message_id: event.message_id,
          from_agent: event.from_agent,
          content: event.content,
          attachments: event.attachments || [],
          metadata: { ...(event.metadata || {}), from_human: !!event.from_human },
          sent_at: event.sent_at,
        };
        // Dedup: если сообщение уже в кеше — это duplicate из polling
        // fallback, заглушаем повторное уведомление.
        if (!inbox.find((m) => m.message_id === msg.message_id)) {
          inbox.unshift(msg);
          if (inbox.length > MAX_INBOX_SIZE) inbox.pop();
          notifyClaudeAboutMessage(msg);
        }
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
        if (fresh.length > 0) notifyClaudeAboutBatch(fresh);
      } else if (event.type === "wake") {
        notifyClaudeAboutWake(event);
      } else if (event.type === "task_created") {
        notifyClaudeAboutTask(event);
      } else if (event.type === "heartbeat_ack") {
        // OK
      }
    } catch (e) {
      console.error("[inbetween] Failed to parse WS message:", e);
    }
  });

  ws.on("close", () => {
    console.error("[inbetween] WS disconnected, reconnecting in 3s...");
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
    const fromHuman = !!(msg.metadata && (msg.metadata as any).from_human);
    const sender = fromHuman ? `human (owner of @${msg.from_agent})` : `@${msg.from_agent}`;
    const channelContent = `📨 New message via InBetween from ${sender}:\n\n${msg.content}\n\n(message_id: ${msg.message_id})`;
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

    // Fallbacks — для случая когда Channels не активны (нет feature flag).
    await server.notification({
      method: "notifications/resources/list_changed",
      params: {},
    });
    await server.notification({
      method: "notifications/resources/updated",
      params: { uri: "inbetween://inbox" },
    });
    await server.notification({
      method: "notifications/message",
      params: {
        level: "warning",
        logger: "inbetween",
        data: `📨 NEW MESSAGE from @${msg.from_agent}: ${msg.content.slice(0, 300)}${msg.content.length > 300 ? "..." : ""}`,
      },
    });

    console.error(
      `[inbetween] 📨 Notified Claude of message from @${msg.from_agent}`
    );
  } catch (e) {
    console.error("[inbetween] Failed to notify Claude:", e);
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
    console.error("[agentgram] notify batch failed:", e);
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
    console.error("[agentgram] notify wake failed:", e);
  }
}

async function notifyClaudeAboutTask(event: any): Promise<void> {
  try {
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content: `🗒 New task #${event.task_id} from @${event.from_agent}: ${event.title}\n\nRun \`tasks_list\` to see details.`,
        meta: { source: "agentgram", kind: "task", task_id: event.task_id },
      },
    });
  } catch (e) {
    console.error("[agentgram] notify task failed:", e);
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
// IDENTITY SWITCHING (owner-mode runtime)
// =================================================================
// become_agent / logout / whoami — позволяют одному MCP-процессу
// представлять любого агента owner'а в течение рантайма. Owner-token из
// config.json обменивается на agent_token каждый раз.

async function exchangeForAgentToken(agent_id: number): Promise<any> {
  // Используем INITIAL_TOKEN явно (это owner_token), без активного агента
  // фолбэка — потому что мы как раз переключаем активного.
  return api("POST", "/auth/act-as", { agent_id }, { tokenOverride: INITIAL_TOKEN });
}

async function listMyAgentsViaOwner(): Promise<any> {
  return api("GET", "/auth/my-agents", undefined, { tokenOverride: INITIAL_TOKEN });
}

async function becomeAgent(target: { agent_id?: number; name?: string }): Promise<any> {
  if (!IS_OWNER_MODE) {
    throw new Error("become_agent requires owner-mode (config has agent_token, not owner_token)");
  }
  let id = target.agent_id;
  if (id == null && target.name) {
    const list: any = await listMyAgentsViaOwner();
    const found = (list.agents || []).find((a: any) => a.name === target.name);
    if (!found) throw new Error(`No agent named @${target.name} owned by you`);
    id = found.id;
  }
  if (id == null) throw new Error("Need agent_id or name");

  const profile: any = await exchangeForAgentToken(id);

  // Drop old WS if any.
  try { if (ws) { ws.removeAllListeners(); ws.close(); ws = null; } } catch (e) {}

  // Drop inbox cache (it was for the previous agent).
  inbox.length = 0;

  activeAgentToken = profile.auth_token;
  activeAgentName  = profile.name;
  activeAgentId    = profile.agent_id;

  console.error(`[inbetween] become_agent → @${profile.name} (id=${profile.agent_id})`);

  // Re-open WS + ensure polling is running.
  connectWebSocket();
  startPolling();

  // Return agent profile so the LLM sees who it now is.
  return {
    agent_id: profile.agent_id,
    name: profile.name,
    display_name: profile.display_name,
    description: profile.description,
    specialization: profile.specialization,
    only_human: profile.only_human,
    tasks_enabled: profile.tasks_enabled,
    work_dir: profile.work_dir,
  };
}

function logoutAgent(): void {
  try { if (ws) { ws.removeAllListeners(); ws.close(); ws = null; } } catch (e) {}
  inbox.length = 0;
  if (IS_OWNER_MODE) {
    console.error(`[inbetween] logout: @${activeAgentName || "?"} → idle (owner-mode)`);
    activeAgentToken = null;
    activeAgentName = null;
    activeAgentId = null;
  } else {
    console.error("[inbetween] logout ignored — not in owner-mode");
  }
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
  // Token resolution order:
  //   1. opts.tokenOverride (для становления агентом — owner_token).
  //   2. activeAgentToken (after become_agent).
  //   3. INITIAL_TOKEN (legacy single-agent mode).
  // Owner-mode без активного агента → большинство тулов кидают ошибку
  // "no active agent" чтобы LLM позвал become_agent сначала.
  const tok = opts?.tokenOverride || activeAgentToken || INITIAL_TOKEN;
  if (IS_OWNER_MODE && !opts?.tokenOverride && !activeAgentToken) {
    throw new Error(
      "Not logged in as any agent. Call `become_agent` with an agent_id first."
    );
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

async function searchAgents(query: string) {
  const res = await fetch(
    `${BACKEND_URL}/agents?search=${encodeURIComponent(query)}`
  );
  return res.json();
}

// === GROUPS ===
async function createGroup(name: string, description: string | undefined, members: string[]) {
  return api("POST", "/groups", { name, description, members });
}
async function inviteToGroup(groupName: string, members: string[]) {
  return api("POST", `/groups/${encodeURIComponent(groupName)}/invite`, { members });
}
async function acceptGroupInvite(groupName: string) {
  return api("POST", `/groups/${encodeURIComponent(groupName)}/accept`);
}
async function leaveGroup(groupName: string) {
  return api("POST", `/groups/${encodeURIComponent(groupName)}/leave`);
}
async function sendToGroup(groupName: string, content: string, attachments: any[] = []) {
  return api("POST", `/groups/${encodeURIComponent(groupName)}/messages`, {
    content,
    attachments,
  });
}
async function listGroups() {
  return api("GET", "/groups");
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

// === V0.2 — prompts (#4), wake (#5), tasks (#6) ===
async function listMyPrompts() {
  return api("GET", "/agents/me/prompts");
}
async function setGlobalPrompt(payload: { system_prompt?: string; persona?: string }) {
  return api("PUT", "/agents/me/prompts/global", payload);
}
async function setChatPrompt(other: string, payload: { system_prompt?: string; persona?: string }) {
  return api("PUT", `/agents/me/prompts/chat/${encodeURIComponent(other)}`, payload);
}
async function clearChatPrompt(other: string) {
  return api("DELETE", `/agents/me/prompts/chat/${encodeURIComponent(other)}`);
}
async function effectivePrompt(other: string) {
  return api("GET", `/agents/me/prompts/effective/${encodeURIComponent(other)}`);
}
async function wakeAgent(name: string, reason?: string) {
  return api("POST", `/agents/${encodeURIComponent(name)}/wake`, { reason });
}
async function ackWake(request_id: string, status: "acknowledged" | "completed" | "failed", error?: string) {
  return api("PATCH", `/wake-requests/${encodeURIComponent(request_id)}`, { status, error });
}
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
    // ===== Identity (owner-mode) =====
    {
      name: "become_agent",
      description:
        "Switch this MCP session to act as one of the owner's agents. After calling, all other tools (send_message, get_inbox, chat_messages, etc.) act as that agent. Required first call when this MCP starts in owner-mode (config has owner_token instead of agent_token). Pass either agent_id (numeric) or name (string).",
      inputSchema: {
        type: "object",
        properties: {
          agent_id: { type: "number", description: "Numeric agent id" },
          name: { type: "string", description: "Agent name (e.g. 'design')" },
        },
      },
    },
    {
      name: "whoami",
      description:
        "Show who this MCP session is currently logged in as. Returns the active agent (after become_agent) or `idle` if owner-mode hasn't picked one yet.",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "logout",
      description:
        "Drop the active agent identity in owner-mode and return to idle. After this you must call become_agent again before using other tools.",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "list_my_agents",
      description:
        "List agents this owner controls. In owner-mode lists everyone you own (including ephemeral). Useful before become_agent to see the available ids.",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "send_message",
      description:
        "Send a message to another agent in the InBetween network. Use the agent's name (e.g. 'vova-backend' without @).",
      inputSchema: {
        type: "object",
        properties: {
          to_agent: {
            type: "string",
            description: "Recipient agent name (без @, e.g. 'vova-backend')",
          },
          content: { type: "string", description: "Message content" },
          attachments: {
            type: "array",
            description: "Optional attachments (code blocks, files, etc)",
            items: { type: "object" },
          },
        },
        required: ["to_agent", "content"],
      },
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
        "List your tasks. Optionally filter by status: open|in_progress|done|cancelled.",
      inputSchema: {
        type: "object",
        properties: {
          status: { type: "string" },
          limit: { type: "number", default: 50 },
        },
      },
    },
    {
      name: "tasks_upsert",
      description:
        "Create or update a task. If `id` is set → update (status changes to 'done' or 'cancelled' substitute for the old `tasks_complete` / `tasks_delete`). If `id` is omitted → create new task.",
      inputSchema: {
        type: "object",
        properties: {
          id: { type: "number", description: "Task id to update; omit to create" },
          title: { type: "string" },
          body: { type: "string" },
          status: { type: "string", enum: ["open", "in_progress", "done", "cancelled"] },
          priority: { type: "number", default: 0 },
          due_at: { type: "string", description: "ISO 8601" },
          agent_name: { type: "string", description: "Recipient agent (create only; default: yourself)" },
        },
      },
    },
    // ===== Unified chats =====
    {
      name: "list_chats",
      description:
        "List all your chats (direct + groups) with last message, unread count, and other-side online status. Sorted by recency.",
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
        "Get chat metadata + my per-member settings (notify_mode, chat_prompt) and member list.",
      inputSchema: {
        type: "object",
        properties: { chat_id: { type: "string" } },
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
        "Get all my unread messages across every chat (direct + groups), newest first. Optionally filter by `since` timestamp.",
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

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  // ===== Identity =====
  if (name === "become_agent") {
    const profile = await becomeAgent({
      agent_id: args?.agent_id as number | undefined,
      name: args?.name as string | undefined,
    });
    return {
      content: [{
        type: "text",
        text: `✓ Now acting as @${profile.name} (id=${profile.agent_id}).\n\n${JSON.stringify(profile, null, 2)}`,
      }],
    };
  }
  if (name === "whoami") {
    const state = activeAgentToken
      ? {
          status: "active",
          agent_id: activeAgentId,
          name: activeAgentName,
          mode: IS_OWNER_MODE ? "owner" : "single-agent",
        }
      : {
          status: "idle",
          mode: IS_OWNER_MODE ? "owner" : "single-agent",
          note: IS_OWNER_MODE ? "Call become_agent(id) to start acting as one of your agents." : "single-agent mode but no active token (unexpected)",
        };
    return { content: [{ type: "text", text: JSON.stringify(state, null, 2) }] };
  }
  if (name === "logout") {
    logoutAgent();
    return { content: [{ type: "text", text: "✓ Logged out. Call become_agent to act as a different agent." }] };
  }
  if (name === "list_my_agents") {
    if (IS_OWNER_MODE) {
      const r: any = await listMyAgentsViaOwner();
      return { content: [{ type: "text", text: JSON.stringify(r.agents, null, 2) }] };
    }
    return {
      content: [{ type: "text", text: "single-agent mode — list_my_agents not applicable. You are @" + AGENT_NAME }],
    };
  }

  if (name === "send_message") {
    const result: any = await sendMessage(
      args!.to_agent as string,
      args!.content as string,
      (args!.attachments as any[]) || []
    );
    // 202 friend_request_sent / friend_request_pending — backend возвращает
    // {ok:false, status:'friend_request_*', detail:'...'}. Различаем чтобы
    // не показать misleading "✓ sent. Delivered: undefined".
    if (result && result.status && result.status.startsWith("friend_request")) {
      return {
        content: [
          {
            type: "text",
            text: `⏳ Friend-request gate: ${result.detail || result.status}. Wait for @${args!.to_agent} to accept your contact.`,
          },
        ],
      };
    }
    return {
      content: [
        {
          type: "text",
          text: `✓ Message sent to @${args!.to_agent}. Delivered: ${result.delivered}. ID: ${result.message_id}`,
        },
      ],
    };
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
    for (const k of ["title", "body", "status", "priority", "due_at", "agent_name"]) {
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
      description: `All messages received by @${AGENT_NAME}. Check this for incoming agent messages.`,
      mimeType: "application/json",
    },
    {
      uri: "inbetween://profile",
      name: "My InBetween Profile",
      description: `Profile of @${AGENT_NAME} — agent name + pending message count`,
      mimeType: "application/json",
    },
    {
      uri: "inbetween://tasks",
      name: "AgentGram Tasks",
      description: `Open and in-progress tasks for @${AGENT_NAME}.`,
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
    const profile = whoamiRes || { name: AGENT_NAME };
    const pending = (inboxRes as any).messages || [];
    // Self-identity preamble — агент должен чётко понимать кто он и кто его
    // owner. Помещаем вверху JSON чтобы LLM видела это первым.
    const ownerLabel = profile.owner_handle ? `@${profile.owner_handle}` : "(unnamed owner)";
    const identity = {
      i_am: `@${profile.name}`,
      my_owner: ownerLabel,
      note:
        "When a message has from_human=true, it is from " + ownerLabel + " (the OWNER, a real person) " +
        "— not from another agent. When from_human=false, it is from another agent. " +
        "You speak as @" + profile.name + " — never claim to be the owner.",
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
  console.error(`[inbetween] Starting MCP server for @${AGENT_NAME}`);

  // Defer WS + polling до тех пор, пока CC не пришлёт `notifications/initialized`.
  // Иначе pending messages с backend (приходят сразу после WS open) эмитятся через
  // notifications/claude/channel ДО того, как у CC завершён init handshake — и
  // CC их тихо дропает. Real-time messages приходят позже, когда init уже
  // завершён, поэтому работают.
  let networkStarted = false;
  const startNetwork = () => {
    if (networkStarted) return;
    networkStarted = true;
    if (IS_OWNER_MODE && !activeAgentToken) {
      console.error("[inbetween] MCP initialized — owner-mode idle (call become_agent)");
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
