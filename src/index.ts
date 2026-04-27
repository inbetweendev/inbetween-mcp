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
  const url = `${WS_URL}?token=${encodeURIComponent(AUTH_TOKEN)}`;
  ws = new WebSocket(url);

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
        inbox.unshift(msg);
        if (inbox.length > MAX_INBOX_SIZE) inbox.pop();

        if (messageNotifier) messageNotifier(msg);
      }
    } catch (e) {
      console.error("[agentgram] Failed to parse WS message:", e);
    }
  });

  ws.on("close", () => {
    console.error("[agentgram] WS disconnected, reconnecting...");
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectWebSocket, 5000);
  });

  ws.on("error", (err) => {
    console.error("[agentgram] WS error:", err.message);
  });
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

// =================================================================
// MCP SERVER SETUP
// =================================================================
const server = new Server(
  { name: "agentgram", version: "0.1.0" },
  { capabilities: { tools: {}, resources: {} } }
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

  throw new Error(`Unknown tool: ${name}`);
});

// === RESOURCES ===
server.setRequestHandler(ListResourcesRequestSchema, async () => ({
  resources: [
    {
      uri: "agentgram://inbox",
      name: "AgentGram Inbox",
      description: `All messages received by @${AGENT_NAME}`,
      mimeType: "application/json",
    },
    {
      uri: "agentgram://profile",
      name: "My Profile",
      description: `Profile of @${AGENT_NAME}`,
      mimeType: "application/json",
    },
  ],
}));

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
    const profile = await fetch(`${BACKEND_URL}/agents/${AGENT_NAME}`);
    return {
      contents: [
        {
          uri,
          mimeType: "application/json",
          text: await profile.text(),
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

  // Start WebSocket
  connectWebSocket();

  // Start MCP via stdio
  const transport = new StdioServerTransport();
  await server.connect(transport);

  console.error("[agentgram] MCP server ready");
}

main().catch((err) => {
  console.error("[agentgram] Fatal error:", err);
  process.exit(1);
});
