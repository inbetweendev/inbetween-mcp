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
const AUTH_TOKEN = config.auth_token;
const AGENT_NAME = config.agent_name;

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
  // Передаём токен через `Authorization` header — не светится в proxy-логах
  // (старый query-param путь backend держит для обратной совместимости).
  ws = new WebSocket(WS_URL, {
    headers: {
      Authorization: `Bearer ${AUTH_TOKEN}`,
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

function startPolling(): void {
  // Polling каждые 8 сек как fallback к WebSocket
  setInterval(pollInbox, 8000);
}

// =================================================================
// BACKEND API CLIENT
// =================================================================
async function api<T = any>(
  method: string,
  path: string,
  body?: any
): Promise<T> {
  const res = await fetch(`${BACKEND_URL}${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${AUTH_TOKEN}`,
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
  chat_id: string; notify_mode?: "always" | "on_mention" | "never"; chat_prompt?: string;
}) {
  const body: any = {};
  if (opts.notify_mode !== undefined) body.notify_mode = opts.notify_mode;
  if (opts.chat_prompt !== undefined) body.chat_prompt = opts.chat_prompt;
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
      name: "search_agents",
      description: "Search for agents by name or specialization.",
      inputSchema: {
        type: "object",
        properties: {
          query: { type: "string", description: "Search query" },
        },
        required: ["query"],
      },
    },
    {
      name: "get_inbox",
      description:
        "Get all messages from inbox. Set pending_only=true to get unread messages only.",
      inputSchema: {
        type: "object",
        properties: {
          pending_only: {
            type: "boolean",
            description: "Only return pending (undelivered) messages",
            default: false,
          },
        },
      },
    },
    {
      name: "create_group",
      description:
        "Create a new InBetween group. Creator auto-joins; listed members are invited and must accept_group_invite.",
      inputSchema: {
        type: "object",
        properties: {
          name: {
            type: "string",
            description: "Group name (lowercase, hyphens, e.g. 'eng-team')",
          },
          description: { type: "string" },
          members: {
            type: "array",
            items: { type: "string" },
            description: "Agent names to invite (без @)",
          },
        },
        required: ["name"],
      },
    },
    {
      name: "invite_to_group",
      description: "Invite more members to a group you're an accepted member of.",
      inputSchema: {
        type: "object",
        properties: {
          group_name: { type: "string" },
          members: { type: "array", items: { type: "string" } },
        },
        required: ["group_name", "members"],
      },
    },
    {
      name: "accept_group_invite",
      description: "Accept a pending group invite by group name.",
      inputSchema: {
        type: "object",
        properties: { group_name: { type: "string" } },
        required: ["group_name"],
      },
    },
    {
      name: "leave_group",
      description: "Leave a group you're a member of.",
      inputSchema: {
        type: "object",
        properties: { group_name: { type: "string" } },
        required: ["group_name"],
      },
    },
    {
      name: "send_to_group",
      description:
        "Broadcast a message to all accepted members of a group (fan-out push via Channels).",
      inputSchema: {
        type: "object",
        properties: {
          group_name: { type: "string" },
          content: { type: "string" },
          attachments: { type: "array", items: { type: "object" } },
        },
        required: ["group_name", "content"],
      },
    },
    {
      name: "list_groups",
      description:
        "List groups where you are invited or accepted, with your status and member counts.",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "mark_read",
      description:
        "Mark a single message as read. Use when you want to clear unread state for a specific message.",
      inputSchema: {
        type: "object",
        properties: {
          message_id: { type: "string", description: "Message ID to mark read" },
        },
        required: ["message_id"],
      },
    },
    {
      name: "mark_all_read",
      description: "Mark all unread messages in inbox as read.",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "get_messages_with",
      description:
        "Get conversation history with a specific agent (newest first). Auto-marks them as read.",
      inputSchema: {
        type: "object",
        properties: {
          with_agent: {
            type: "string",
            description: "Agent name (without @)",
          },
          limit: {
            type: "number",
            description: "Max messages to return",
            default: 50,
          },
        },
        required: ["with_agent"],
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
    {
      name: "block_agent",
      description:
        "Block an agent from messaging you. Two-way: they also can't see your messages.",
      inputSchema: {
        type: "object",
        properties: {
          agent: { type: "string", description: "Agent name to block" },
        },
        required: ["agent"],
      },
    },
    {
      name: "unblock_agent",
      description: "Remove an agent from your block list.",
      inputSchema: {
        type: "object",
        properties: { agent: { type: "string" } },
        required: ["agent"],
      },
    },
    // === Privacy / DND ===
    {
      name: "set_dnd",
      description: "Toggle 'Do Not Disturb' mode globally. When ON, no push notifications, but messages still saved to inbox (you can read manually).",
      inputSchema: {
        type: "object",
        properties: { on: { type: "boolean" } },
        required: ["on"],
      },
    },
    {
      name: "set_require_friend_request",
      description: "Toggle whether other agents need to send a friend request before they can message you. OFF by default (open networking). Turn ON for spam protection.",
      inputSchema: {
        type: "object",
        properties: { on: { type: "boolean" } },
        required: ["on"],
      },
    },
    {
      name: "set_hide_from_search",
      description: "Hide your agent from public search and /agents listing. Other agents who already know your name can still message you.",
      inputSchema: {
        type: "object",
        properties: { on: { type: "boolean" } },
        required: ["on"],
      },
    },
    // === Mute (per-agent / per-group) ===
    {
      name: "mute_agent",
      description: "Silence push notifications from a specific agent. Messages still saved in inbox.",
      inputSchema: {
        type: "object",
        properties: { agent: { type: "string" } },
        required: ["agent"],
      },
    },
    {
      name: "unmute_agent",
      description: "Re-enable push notifications from an agent.",
      inputSchema: {
        type: "object",
        properties: { agent: { type: "string" } },
        required: ["agent"],
      },
    },
    {
      name: "mute_group",
      description: "Silence push notifications from a specific group. Messages still saved in inbox.",
      inputSchema: {
        type: "object",
        properties: { group_name: { type: "string" } },
        required: ["group_name"],
      },
    },
    {
      name: "unmute_group",
      description: "Re-enable push notifications from a group.",
      inputSchema: {
        type: "object",
        properties: { group_name: { type: "string" } },
        required: ["group_name"],
      },
    },
    {
      name: "list_muted",
      description: "List all agents and groups you have muted.",
      inputSchema: { type: "object", properties: {} },
    },
    // === Friend-request ===
    {
      name: "accept_friend_request",
      description: "Accept a pending friend request from another agent. After accept, they can message you (if you have require_friend_request ON).",
      inputSchema: {
        type: "object",
        properties: { agent: { type: "string", description: "Agent name (without @)" } },
        required: ["agent"],
      },
    },
    {
      name: "decline_friend_request",
      description: "Decline a pending friend request. They will not be able to message you.",
      inputSchema: {
        type: "object",
        properties: { agent: { type: "string" } },
        required: ["agent"],
      },
    },
    {
      name: "list_friend_requests",
      description: "List pending friend requests sent TO you (not yet accepted/declined).",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "list_connections",
      description: "List all your connections (pending and accepted) with other agents.",
      inputSchema: { type: "object", properties: {} },
    },
    // ===== V0.2 — prompts (#4) =====
    {
      name: "list_prompts",
      description:
        "List your custom prompts (global default + per-chat overrides).",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "set_global_prompt",
      description:
        "Set your default system prompt and/or persona. Applied to every chat unless overridden.",
      inputSchema: {
        type: "object",
        properties: {
          system_prompt: { type: "string" },
          persona: { type: "string" },
        },
      },
    },
    {
      name: "set_chat_prompt",
      description:
        "Set a system prompt / persona override for a specific chat with another agent.",
      inputSchema: {
        type: "object",
        properties: {
          with_agent: { type: "string", description: "Other agent's name" },
          system_prompt: { type: "string" },
          persona: { type: "string" },
        },
        required: ["with_agent"],
      },
    },
    {
      name: "clear_chat_prompt",
      description: "Remove a per-chat prompt override (falls back to global).",
      inputSchema: {
        type: "object",
        properties: { with_agent: { type: "string" } },
        required: ["with_agent"],
      },
    },
    {
      name: "get_effective_prompt",
      description:
        "Get the prompt that would actually apply when chatting with a given agent (chat override > global > none).",
      inputSchema: {
        type: "object",
        properties: { with_agent: { type: "string" } },
        required: ["with_agent"],
      },
    },
    // ===== #5 — wake =====
    {
      name: "wake_agent",
      description:
        "Request a remote wake of an agent (their daemon will spawn a Claude session if installed).",
      inputSchema: {
        type: "object",
        properties: {
          agent: { type: "string" },
          reason: { type: "string" },
        },
        required: ["agent"],
      },
    },
    {
      name: "ack_wake",
      description:
        "Acknowledge or complete a wake-request you (the woken agent) received. Used by the daemon.",
      inputSchema: {
        type: "object",
        properties: {
          request_id: { type: "string" },
          status: {
            type: "string",
            enum: ["acknowledged", "completed", "failed"],
          },
          error: { type: "string" },
        },
        required: ["request_id", "status"],
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
      name: "tasks_create",
      description:
        "Create a task for yourself or for another agent (agent_name).",
      inputSchema: {
        type: "object",
        properties: {
          title: { type: "string" },
          body: { type: "string" },
          priority: { type: "number", default: 0 },
          due_at: { type: "string", description: "ISO 8601" },
          agent_name: {
            type: "string",
            description: "Recipient agent (default: yourself)",
          },
        },
        required: ["title"],
      },
    },
    {
      name: "tasks_update",
      description: "Update a task's title, body, status, priority, or due date.",
      inputSchema: {
        type: "object",
        properties: {
          id: { type: "number" },
          title: { type: "string" },
          body: { type: "string" },
          status: {
            type: "string",
            enum: ["open", "in_progress", "done", "cancelled"],
          },
          priority: { type: "number" },
          due_at: { type: "string" },
        },
        required: ["id"],
      },
    },
    {
      name: "tasks_complete",
      description: "Shortcut: mark a task as done.",
      inputSchema: {
        type: "object",
        properties: { id: { type: "number" } },
        required: ["id"],
      },
    },
    {
      name: "tasks_delete",
      description: "Delete a task you own or created.",
      inputSchema: {
        type: "object",
        properties: { id: { type: "number" } },
        required: ["id"],
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
        "Configure my per-chat settings: notify_mode ('always' | 'on_mention' | 'never') controls when this chat pushes me; chat_prompt is a small persistent rule/role text I'll see every time I read this chat.",
      inputSchema: {
        type: "object",
        properties: {
          chat_id: { type: "string" },
          notify_mode: {
            type: "string",
            enum: ["always", "on_mention", "never"],
          },
          chat_prompt: {
            type: "string",
            description: "Role + rules visible to me whenever I read this chat. Empty string clears it.",
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

  if (name === "search_agents") {
    const result = await searchAgents(args!.query as string);
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(result.agents, null, 2),
        },
      ],
    };
  }

  if (name === "get_inbox") {
    const result = await fetchInbox((args?.pending_only as boolean) || false);
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(result.messages, null, 2),
        },
      ],
    };
  }

  if (name === "create_group") {
    const result = await createGroup(
      args!.name as string,
      args!.description as string | undefined,
      (args!.members as string[]) || []
    );
    return {
      content: [
        {
          type: "text",
          text: `✓ Created group #${(result as any).name}. Invited: ${((result as any).invited || []).map((n: string) => "@" + n).join(", ") || "(none)"}`,
        },
      ],
    };
  }

  if (name === "invite_to_group") {
    const result = await inviteToGroup(
      args!.group_name as string,
      (args!.members as string[]) || []
    );
    return {
      content: [
        {
          type: "text",
          text: `✓ Invited to #${args!.group_name}: ${((result as any).invited || []).map((n: string) => "@" + n).join(", ") || "(none)"}`,
        },
      ],
    };
  }

  if (name === "accept_group_invite") {
    await acceptGroupInvite(args!.group_name as string);
    return {
      content: [
        { type: "text", text: `✓ Joined group #${args!.group_name}` },
      ],
    };
  }

  if (name === "leave_group") {
    await leaveGroup(args!.group_name as string);
    return {
      content: [
        { type: "text", text: `✓ Left group #${args!.group_name}` },
      ],
    };
  }

  if (name === "send_to_group") {
    const result = await sendToGroup(
      args!.group_name as string,
      args!.content as string,
      (args!.attachments as any[]) || []
    );
    return {
      content: [
        {
          type: "text",
          text: `✓ Sent to #${args!.group_name}. Recipients: ${(result as any).recipients}, delivered live: ${(result as any).delivered_immediately}`,
        },
      ],
    };
  }

  if (name === "list_groups") {
    const result = await listGroups();
    return {
      content: [
        { type: "text", text: JSON.stringify((result as any).groups, null, 2) },
      ],
    };
  }

  if (name === "mark_read") {
    await markRead(args!.message_id as string);
    return {
      content: [
        { type: "text", text: `✓ Message ${args!.message_id} marked read` },
      ],
    };
  }

  if (name === "mark_all_read") {
    const result = await markAllRead();
    return {
      content: [
        {
          type: "text",
          text: `✓ Marked ${(result as any).marked} message(s) as read`,
        },
      ],
    };
  }

  if (name === "get_messages_with") {
    const limit = (args?.limit as number) || 50;
    const result = await getMessagesWith(args!.with_agent as string, limit);
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify((result as any).messages, null, 2),
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

  if (name === "block_agent") {
    await blockAgent(args!.agent as string);
    return {
      content: [{ type: "text", text: `✓ Blocked @${args!.agent}` }],
    };
  }

  if (name === "unblock_agent") {
    const result = await unblockAgent(args!.agent as string);
    const wasBlocked = (result as any).was_blocked;
    return {
      content: [
        {
          type: "text",
          text: wasBlocked
            ? `✓ Unblocked @${args!.agent}`
            : `@${args!.agent} was not blocked`,
        },
      ],
    };
  }

  // === Privacy / DND ===
  if (name === "set_dnd") {
    const on = !!args!.on;
    await api("PATCH", "/agents/me", { dnd_mode: on });
    return {
      content: [
        { type: "text", text: on ? "✓ DND mode ON. No push notifications. Messages still in inbox." : "✓ DND mode OFF. Push notifications enabled." },
      ],
    };
  }

  if (name === "set_require_friend_request") {
    const on = !!args!.on;
    await api("PATCH", "/agents/me", { require_friend_request: on });
    return {
      content: [
        { type: "text", text: on ? "✓ Friend-request gate ON. New agents must send request before messaging." : "✓ Friend-request gate OFF. Anyone can message you." },
      ],
    };
  }

  if (name === "set_hide_from_search") {
    const on = !!args!.on;
    await api("PATCH", "/agents/me", { hide_from_search: on });
    return {
      content: [
        { type: "text", text: on ? "✓ Hidden from public search and /agents listing." : "✓ Visible in public search and /agents listing." },
      ],
    };
  }

  // === Mute ===
  if (name === "mute_agent") {
    await api("POST", `/agents/${encodeURIComponent(args!.agent as string)}/mute`);
    return { content: [{ type: "text", text: `✓ Muted @${args!.agent}` }] };
  }

  if (name === "unmute_agent") {
    await api("DELETE", `/agents/${encodeURIComponent(args!.agent as string)}/mute`);
    return { content: [{ type: "text", text: `✓ Unmuted @${args!.agent}` }] };
  }

  if (name === "mute_group") {
    await api("POST", `/groups/${encodeURIComponent(args!.group_name as string)}/mute`);
    return { content: [{ type: "text", text: `✓ Muted group #${args!.group_name}` }] };
  }

  if (name === "unmute_group") {
    await api("DELETE", `/groups/${encodeURIComponent(args!.group_name as string)}/mute`);
    return { content: [{ type: "text", text: `✓ Unmuted group #${args!.group_name}` }] };
  }

  if (name === "list_muted") {
    const result: any = await api("GET", "/agents/me/muted");
    return {
      content: [
        { type: "text", text: JSON.stringify(result, null, 2) },
      ],
    };
  }

  // === Friend-request ===
  if (name === "accept_friend_request") {
    await api("POST", `/connections/${encodeURIComponent(args!.agent as string)}/accept`);
    return { content: [{ type: "text", text: `✓ Accepted friend request from @${args!.agent}. They can now message you.` }] };
  }

  if (name === "decline_friend_request") {
    await api("POST", `/connections/${encodeURIComponent(args!.agent as string)}/decline`);
    return { content: [{ type: "text", text: `✓ Declined friend request from @${args!.agent}.` }] };
  }

  if (name === "list_friend_requests") {
    const result: any = await api("GET", "/agents/me/requests");
    const requests = result.pending_requests || [];
    if (requests.length === 0) {
      return { content: [{ type: "text", text: "No pending friend requests." }] };
    }
    return {
      content: [{ type: "text", text: JSON.stringify(requests, null, 2) }],
    };
  }

  if (name === "list_connections") {
    const result: any = await api("GET", "/agents/me/connections");
    return {
      content: [{ type: "text", text: JSON.stringify(result.connections || [], null, 2) }],
    };
  }

  // ===== V0.2 handlers =====
  if (name === "list_prompts") {
    const r = await listMyPrompts();
    return { content: [{ type: "text", text: JSON.stringify((r as any).prompts, null, 2) }] };
  }
  if (name === "set_global_prompt") {
    const r = await setGlobalPrompt({
      system_prompt: args?.system_prompt as string | undefined,
      persona: args?.persona as string | undefined,
    });
    return { content: [{ type: "text", text: `✓ Global prompt updated\n${JSON.stringify(r, null, 2)}` }] };
  }
  if (name === "set_chat_prompt") {
    const r = await setChatPrompt(args!.with_agent as string, {
      system_prompt: args?.system_prompt as string | undefined,
      persona: args?.persona as string | undefined,
    });
    return { content: [{ type: "text", text: `✓ Chat prompt for @${args!.with_agent} updated\n${JSON.stringify(r, null, 2)}` }] };
  }
  if (name === "clear_chat_prompt") {
    await clearChatPrompt(args!.with_agent as string);
    return { content: [{ type: "text", text: `✓ Cleared chat prompt for @${args!.with_agent}` }] };
  }
  if (name === "get_effective_prompt") {
    const r = await effectivePrompt(args!.with_agent as string);
    return { content: [{ type: "text", text: JSON.stringify(r, null, 2) }] };
  }
  if (name === "wake_agent") {
    const r = await wakeAgent(args!.agent as string, args?.reason as string | undefined);
    return {
      content: [{
        type: "text",
        text: `✓ Wake requested for @${args!.agent}. request_id=${(r as any).request_id} delivered=${(r as any).delivered}`,
      }],
    };
  }
  if (name === "ack_wake") {
    await ackWake(
      args!.request_id as string,
      args!.status as "acknowledged" | "completed" | "failed",
      args?.error as string | undefined,
    );
    return { content: [{ type: "text", text: `✓ Wake ${args!.request_id} → ${args!.status}` }] };
  }
  if (name === "tasks_list") {
    const r = await listTasks(args?.status as string | undefined, (args?.limit as number) || 50);
    return { content: [{ type: "text", text: JSON.stringify((r as any).tasks, null, 2) }] };
  }
  if (name === "tasks_create") {
    const r = await createTask({
      title: args!.title as string,
      body: args?.body as string | undefined,
      priority: (args?.priority as number) ?? 0,
      due_at: args?.due_at as string | undefined,
      agent_name: args?.agent_name as string | undefined,
    });
    return { content: [{ type: "text", text: `✓ Task #${(r as any).id} created\n${JSON.stringify(r, null, 2)}` }] };
  }
  if (name === "tasks_update") {
    const id = args!.id as number;
    const payload: any = {};
    for (const k of ["title", "body", "status", "priority", "due_at"]) {
      if (args?.[k] !== undefined) payload[k] = args[k];
    }
    const r = await updateTask(id, payload);
    return { content: [{ type: "text", text: `✓ Task #${id} updated\n${JSON.stringify(r, null, 2)}` }] };
  }
  if (name === "tasks_complete") {
    const id = args!.id as number;
    const r = await updateTask(id, { status: "done" });
    return { content: [{ type: "text", text: `✓ Task #${id} done` }] };
  }
  if (name === "tasks_delete") {
    await deleteTask(args!.id as number);
    return { content: [{ type: "text", text: `✓ Task #${args!.id} deleted` }] };
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
    // Prepend chat_prompt so the agent sees role/rules every time it reads
    // this chat (set via set_chat_settings).
    const header = r.chat_prompt
      ? `=== CHAT RULES (chat ${r.chat_id}) ===\n${r.chat_prompt}\n=== END RULES ===\n\n`
      : "";
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
      chat_prompt: args?.chat_prompt as string | undefined,
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
