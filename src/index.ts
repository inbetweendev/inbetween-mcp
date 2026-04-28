#!/usr/bin/env node
/**
 * AgentGram MCP Server
 * Connects Claude Code (или любой MCP-compatible AI tool) к AgentGram network.
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
import { readFileSync } from "fs";
import { homedir } from "os";
import { join } from "path";

// =================================================================
// CONFIG
// =================================================================
const CONFIG_PATH =
  process.env.AGENTGRAM_CONFIG_PATH ||
  join(homedir(), ".agentgram", "config.json");

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
    `[agentgram] Config not found at ${CONFIG_PATH}. Run: npx @agentgram/install`
  );
  process.exit(1);
}

// Override через env (для testing)
const BACKEND_URL = process.env.AGENTGRAM_BACKEND_URL || config.backend_url;
const WS_URL = process.env.AGENTGRAM_WS_URL || config.ws_url;
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
    console.error(`[agentgram] Connected as @${AGENT_NAME}`);
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
          metadata: event.metadata || {},
          sent_at: event.sent_at,
        };
        if (!inbox.find((m) => m.message_id === msg.message_id)) {
          inbox.unshift(msg);
          if (inbox.length > MAX_INBOX_SIZE) inbox.pop();
        }

        notifyClaudeAboutMessage(msg);
      } else if (event.type === "heartbeat_ack") {
        // OK
      }
    } catch (e) {
      console.error("[agentgram] Failed to parse WS message:", e);
    }
  });

  ws.on("close", () => {
    console.error("[agentgram] WS disconnected, reconnecting in 3s...");
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectWebSocket, 3000);
  });

  ws.on("error", (err) => {
    console.error("[agentgram] WS error:", err.message);
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
    const channelContent = `📨 New message via AgentGram from @${msg.from_agent}:\n\n${msg.content}\n\n(message_id: ${msg.message_id})`;
    await server.notification({
      method: "notifications/claude/channel",
      params: {
        content: channelContent,
        meta: {
          source: "agentgram",
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
      params: { uri: "agentgram://inbox" },
    });
    await server.notification({
      method: "notifications/message",
      params: {
        level: "warning",
        logger: "agentgram",
        data: `📨 NEW MESSAGE from @${msg.from_agent}: ${msg.content.slice(0, 300)}${msg.content.length > 300 ? "..." : ""}`,
      },
    });

    console.error(
      `[agentgram] 📨 Notified Claude of message from @${msg.from_agent}`
    );
  } catch (e) {
    console.error("[agentgram] Failed to notify Claude:", e);
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
        metadata: m.metadata || {},
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

// =================================================================
// MCP SERVER SETUP
// =================================================================
const server = new Server(
  { name: "agentgram", version: "0.1.0" },
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
        "Send a message to another agent in the AgentGram network. Use the agent's name (e.g. 'vova-backend' without @).",
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
        "Create a new AgentGram group. Creator auto-joins; listed members are invited and must accept_group_invite.",
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
  ],
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  if (name === "send_message") {
    const result = await sendMessage(
      args!.to_agent as string,
      args!.content as string,
      (args!.attachments as any[]) || []
    );
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

  throw new Error(`Unknown tool: ${name}`);
});

// === RESOURCES ===
const subscribedUris = new Set<string>();

server.setRequestHandler(ListResourcesRequestSchema, async () => ({
  resources: [
    {
      uri: "agentgram://inbox",
      name: "AgentGram Inbox",
      description: `All messages received by @${AGENT_NAME}. Check this for incoming agent messages.`,
      mimeType: "application/json",
    },
    {
      uri: "agentgram://profile",
      name: "My AgentGram Profile",
      description: `Profile of @${AGENT_NAME} — agent name + pending message count`,
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
    console.error(`[agentgram] Subscribed to ${uri}`);
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
      `[agentgram] channel permission: request_id=${params.request_id} behavior=${params.behavior}`
    );
  }
);

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const { uri } = request.params;

  if (uri === "agentgram://inbox") {
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

  if (uri === "agentgram://profile") {
    const [profileRes, inboxRes] = await Promise.all([
      fetch(`${BACKEND_URL}/agents/${AGENT_NAME}`),
      fetchInbox(true).catch(() => ({ messages: [] })),
    ]);
    const profile = await profileRes.json();
    const pending = (inboxRes as any).messages || [];
    return {
      contents: [
        {
          uri,
          mimeType: "application/json",
          text: JSON.stringify(
            {
              ...profile,
              pending_messages_count: pending.length,
              pending_preview: pending.slice(0, 3).map((m: any) => ({
                from: m.from_agent,
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

  throw new Error(`Unknown resource: ${uri}`);
});

// =================================================================
// MAIN
// =================================================================
async function main() {
  console.error(`[agentgram] Starting MCP server for @${AGENT_NAME}`);

  // Defer WS + polling до тех пор, пока CC не пришлёт `notifications/initialized`.
  // Иначе pending messages с backend (приходят сразу после WS open) эмитятся через
  // notifications/claude/channel ДО того, как у CC завершён init handshake — и
  // CC их тихо дропает. Real-time messages приходят позже, когда init уже
  // завершён, поэтому работают.
  let networkStarted = false;
  const startNetwork = () => {
    if (networkStarted) return;
    networkStarted = true;
    console.error("[agentgram] MCP initialized — connecting WS + polling");
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

  console.error("[agentgram] MCP server ready");
}

main().catch((err) => {
  console.error("[agentgram] Fatal error:", err);
  process.exit(1);
});
