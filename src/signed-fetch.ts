/**
 * Wrap `fetch` with the headers the InBetween backend requires from
 * official clients: `X-Client-Version` (gates against MIN_CLIENT_VERSION)
 * and `X-Signature`/`X-Timestamp`/`X-Nonce` (HMAC over METHOD + PATH +
 * TIMESTAMP + NONCE + sha256(BODY)).
 *
 * The HMAC secret is baked into the published bundle at build time via
 * `esbuild --define process.env.INBETWEEN_BUILD_SECRET=...`. In local
 * dev (without a build) the env var is read at runtime — backend with
 * empty INBETWEEN_HMAC_SECRETS treats unsigned traffic as ok, so dev
 * doesn't need the secret to work against a local backend.
 *
 * Backend signs the same path-with-query that we do here.
 */
import { createHash, createHmac, randomBytes } from "crypto";

const BUILD_SECRET = process.env.INBETWEEN_BUILD_SECRET || "";
const CLIENT_VERSION = process.env.INBETWEEN_CLIENT_VERSION || "0.0.0-dev";

function sign(
  method: string,
  pathWithQuery: string,
  body: string,
): { sig: string; ts: string; nonce: string } {
  const ts = String(Math.floor(Date.now() / 1000));
  const nonce = randomBytes(16).toString("hex");
  const bodyHash = createHash("sha256").update(body).digest("hex");
  const base = `${method.toUpperCase()}\n${pathWithQuery}\n${ts}\n${nonce}\n${bodyHash}`;
  const sig = createHmac("sha256", BUILD_SECRET).update(base).digest("hex");
  return { sig, ts, nonce };
}

export async function signedFetch(
  fullUrl: string,
  init: RequestInit = {},
): Promise<Response> {
  const url = new URL(fullUrl);
  const method = (init.method || "GET").toUpperCase();
  const body = typeof init.body === "string" ? init.body : "";
  const headers = new Headers(init.headers || {});
  headers.set("X-Client-Version", CLIENT_VERSION);
  if (BUILD_SECRET) {
    const { sig, ts, nonce } = sign(
      method,
      url.pathname + (url.search || ""),
      body,
    );
    headers.set("X-Signature", sig);
    headers.set("X-Timestamp", ts);
    headers.set("X-Nonce", nonce);
  }
  return fetch(fullUrl, { ...init, headers });
}

/** Headers to attach to the outbound WebSocket upgrade (HTTP middleware
 * does not run on the WS scope, so the gate has to read the same header
 * directly in ws/endpoint.py). HMAC isn't applied to the WS handshake —
 * the auth boundary there is the agent_token. */
export function wsHandshakeHeaders(): Record<string, string> {
  return { "X-Client-Version": CLIENT_VERSION };
}

export { CLIENT_VERSION };
