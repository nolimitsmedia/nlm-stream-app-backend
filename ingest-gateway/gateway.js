"use strict";

const http = require("http");
const crypto = require("crypto");

const PORT = clampPort(process.env.GATEWAY_CONTROL_PORT || 5092, 5092);
const BACKEND_URL = String(process.env.NLM_BACKEND_URL || "http://127.0.0.1:5000").replace(/\/+$/, "");
const GATEWAY_TOKEN = String(process.env.INGEST_GATEWAY_TOKEN || "").trim();
const RESOLVE_TIMEOUT_MS = Math.max(500, Number(process.env.GATEWAY_RESOLVE_TIMEOUT_MS || 3000));
const CACHE_TTL_MS = Math.max(1000, Number(process.env.GATEWAY_ROUTE_CACHE_TTL_MS || 15000));
const UPSTREAM_RTMP_PORT = clampPort(process.env.GATEWAY_UPSTREAM_RTMP_PORT || 1935, 1935);
const MAX_BODY_BYTES = Math.max(4096, Number(process.env.GATEWAY_MAX_BODY_BYTES || 65536));

const routeCache = new Map();

function clampPort(value, fallback) {
  const n = Number(value);
  return Number.isInteger(n) && n >= 1 && n <= 65535 ? n : fallback;
}

function streamLogId(value) {
  const key = String(value || "");
  if (!key) return "[none]";
  return `[key:${crypto.createHash("sha256").update(key).digest("hex").slice(0, 10)}]`;
}

function normalizeHost(value) {
  const host = String(value || "").trim().replace(/^\[|\]$/g, "");
  if (!host || /[\s\/?#]/.test(host)) return null;
  return host;
}

function formatHost(host) {
  return host.includes(":") ? `[${host}]` : host;
}

function inferIngressProtocol(payload) {
  const tcUrl = String(payload?.tcUrl || payload?.tc_url || "").toLowerCase();
  if (tcUrl.startsWith("rtmps://")) return "rtmps";
  if (tcUrl.startsWith("rtmp://")) return "rtmp";
  // SRS normalizes SRT ingest into its live stream graph. The backend resolver
  // currently uses protocol only to choose the assigned node target; the gateway
  // always forwards upstream using RTMP, so RTMP is the safe resolver contract.
  return "rtmp";
}

function getCached(streamKey) {
  const entry = routeCache.get(streamKey);
  if (!entry) return null;
  if (Date.now() - entry.cachedAt > CACHE_TTL_MS) {
    routeCache.delete(streamKey);
    return null;
  }
  return entry.value;
}

function setCached(streamKey, value) {
  routeCache.set(streamKey, { cachedAt: Date.now(), value });
}

async function resolveRoute(streamKey, protocol) {
  const cached = getCached(streamKey);
  if (cached) return cached;

  if (GATEWAY_TOKEN.length < 32) {
    const error = new Error("gateway_auth_not_configured");
    error.code = "gateway_auth_not_configured";
    throw error;
  }

  const response = await fetch(`${BACKEND_URL}/api/internal/ingest/resolve`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-NLM-Ingest-Gateway-Token": GATEWAY_TOKEN,
    },
    body: JSON.stringify({ stream_key: streamKey, protocol }),
    signal: AbortSignal.timeout(RESOLVE_TIMEOUT_MS),
  });

  let data = null;
  try {
    data = await response.json();
  } catch {
    data = null;
  }

  if (!response.ok || data?.ok !== true || data?.allowed !== true) {
    const error = new Error(String(data?.reason || `resolver_http_${response.status}`));
    error.code = String(data?.reason || `resolver_http_${response.status}`);
    throw error;
  }

  const nodeId = Number(data.media_node_id ?? data.media_node?.id ?? data.target?.media_node_id ?? 0);
  const host = normalizeHost(data.target?.host ?? data.media_node?.host ?? data.ingest_host);
  const port = clampPort(data.target?.port ?? data.target?.rtmp_port ?? UPSTREAM_RTMP_PORT, UPSTREAM_RTMP_PORT);

  if (!Number.isInteger(nodeId) || nodeId <= 0 || !host) {
    const error = new Error("resolver_target_invalid");
    error.code = "resolver_target_invalid";
    throw error;
  }

  const value = { nodeId, host, port };
  setCached(streamKey, value);
  return value;
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(Object.assign(new Error("body_too_large"), { code: "body_too_large" }));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      try {
        const text = Buffer.concat(chunks).toString("utf8");
        resolve(text ? JSON.parse(text) : {});
      } catch {
        reject(Object.assign(new Error("invalid_json"), { code: "invalid_json" }));
      }
    });
    req.on("error", reject);
  });
}

function json(res, status, body) {
  const encoded = Buffer.from(JSON.stringify(body));
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Content-Length": encoded.length,
    "Cache-Control": "no-store",
  });
  res.end(encoded);
}

async function handlePublish(payload, res) {
  const streamKey = String(payload?.stream || "").trim();
  const app = String(payload?.app || "").trim();
  if (app !== "live" || !streamKey) {
    console.warn(`[GATEWAY] publish rejected app=${app || "[none]"} stream=${streamLogId(streamKey)} reason=invalid_publish_identity`);
    return json(res, 403, { code: 1 });
  }

  try {
    const route = await resolveRoute(streamKey, inferIngressProtocol(payload));
    console.log(`[GATEWAY] publish allowed stream=${streamLogId(streamKey)} node=${route.nodeId}`);
    return json(res, 200, { code: 0 });
  } catch (error) {
    console.warn(`[GATEWAY] publish rejected stream=${streamLogId(streamKey)} reason=${error.code || error.message}`);
    return json(res, 403, { code: 1 });
  }
}

async function handleForward(payload, res) {
  const streamKey = String(payload?.stream || "").trim();
  const app = String(payload?.app || "").trim();
  if (app !== "live" || !streamKey) return json(res, 200, { code: 0, data: { urls: [] } });

  try {
    const route = await resolveRoute(streamKey, inferIngressProtocol(payload));
    const target = `rtmp://${formatHost(route.host)}:${route.port}/live/${encodeURIComponent(streamKey)}`;
    console.log(`[GATEWAY] forward route stream=${streamLogId(streamKey)} node=${route.nodeId}`);
    return json(res, 200, { code: 0, data: { urls: [target] } });
  } catch (error) {
    console.warn(`[GATEWAY] forward disabled stream=${streamLogId(streamKey)} reason=${error.code || error.message}`);
    return json(res, 200, { code: 0, data: { urls: [] } });
  }
}

function handleUnpublish(payload, res) {
  const streamKey = String(payload?.stream || "").trim();
  if (streamKey) routeCache.delete(streamKey);
  console.log(`[GATEWAY] unpublish stream=${streamLogId(streamKey)} cache=cleared`);
  return json(res, 200, { code: 0 });
}

const server = http.createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/health") {
    return json(res, 200, {
      ok: true,
      service: "nlm-ingest-gateway-control",
      resolver_configured: GATEWAY_TOKEN.length >= 32,
      backend_url_configured: Boolean(BACKEND_URL),
      cached_routes: routeCache.size,
    });
  }

  if (req.method !== "POST") return json(res, 404, { code: 404 });

  try {
    const payload = await readJson(req);
    if (req.url === "/srs/on-publish") return await handlePublish(payload, res);
    if (req.url === "/srs/on-unpublish") return handleUnpublish(payload, res);
    if (req.url === "/srs/forward") return await handleForward(payload, res);
    return json(res, 404, { code: 404 });
  } catch (error) {
    console.warn(`[GATEWAY] request rejected reason=${error.code || error.message}`);
    return json(res, 400, { code: 1 });
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[GATEWAY] control service listening port=${PORT}`);
  console.log(`[GATEWAY] resolver=${GATEWAY_TOKEN.length >= 32 ? "configured" : "not_configured"}`);
});

function shutdown(signal) {
  console.log(`[GATEWAY] ${signal} received; shutting down.`);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5000).unref();
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
