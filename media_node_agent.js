"use strict";

require("dotenv").config();

const http = require("http");
const os = require("os");
const crypto = require("crypto");
const { execFileSync } = require("child_process");

const {
  getLocalSystemMetrics,
  getSrsHealth,
  getFfmpegProcessCount,
} = require("./media_node_service");

const AGENT_VERSION = "4A.1";
const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT = 5091;
const DEFAULT_SRS_API_URL = "http://127.0.0.1:1985";
const MAX_RESPONSE_BYTES = 1024 * 1024;

const host = String(process.env.MEDIA_NODE_AGENT_HOST || DEFAULT_HOST).trim();
const port = Math.max(
  1,
  Math.min(65535, Number(process.env.MEDIA_NODE_AGENT_PORT || DEFAULT_PORT)),
);
const srsApiUrl = String(
  process.env.MEDIA_NODE_AGENT_SRS_API_URL ||
    process.env.SRS_API_URL ||
    DEFAULT_SRS_API_URL,
).replace(/\/$/, "");
const nodeId = String(process.env.MEDIA_NODE_ID || "").trim();
const nodeName = String(process.env.MEDIA_NODE_NAME || os.hostname()).trim();
const agentToken = String(process.env.MEDIA_NODE_AGENT_TOKEN || "").trim();

if (!agentToken || agentToken.length < 32) {
  console.error(
    "[MediaNodeAgent] MEDIA_NODE_AGENT_TOKEN is required and must be at least 32 characters.",
  );
  process.exit(1);
}

function timingSafeTokenEqual(receivedToken) {
  const expected = Buffer.from(agentToken, "utf8");
  const received = Buffer.from(String(receivedToken || ""), "utf8");

  if (expected.length !== received.length) {
    return false;
  }

  return crypto.timingSafeEqual(expected, received);
}

function isAuthorized(req) {
  const header = String(req.headers.authorization || "");
  const match = header.match(/^Bearer\s+(.+)$/i);
  return Boolean(match && timingSafeTokenEqual(match[1].trim()));
}

function sendJson(res, statusCode, payload) {
  const body = Buffer.from(JSON.stringify(payload));

  if (body.length > MAX_RESPONSE_BYTES) {
    const overflow = Buffer.from(
      JSON.stringify({
        ok: false,
        error: "Agent response exceeded safety limit",
      }),
    );
    res.writeHead(500, {
      "Content-Type": "application/json; charset=utf-8",
      "Content-Length": overflow.length,
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
    });
    res.end(overflow);
    return;
  }

  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": body.length,
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
  });
  res.end(body);
}

function getDiskFreeBytes() {
  try {
    const output = execFileSync("df", ["-Pk", "/"], {
      encoding: "utf8",
      timeout: 3000,
    })
      .trim()
      .split("\n");

    const row = output[output.length - 1] || "";
    const parts = row.trim().split(/\s+/);
    const availableKb = Number(parts[3] || 0);

    return Number.isFinite(availableKb) ? availableKb * 1024 : null;
  } catch {
    return null;
  }
}

async function fetchJson(url, timeoutMs = 5000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  timer.unref?.();

  try {
    const response = await fetch(url, { signal: controller.signal });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

async function getSanitizedStreams() {
  const data = await fetchJson(`${srsApiUrl}/api/v1/streams`);
  const streams = Array.isArray(data?.streams) ? data.streams : [];

  return streams.map((stream) => ({
    id: stream?.id ?? null,
    vhost: stream?.vhost ?? null,
    app: stream?.app ?? null,
    name: stream?.name ?? null,
    publish_active: stream?.publish?.active === true,
    clients: Number(stream?.clients || 0),
    frames: Number(stream?.frames || 0),
    send_bytes: Number(stream?.send_bytes || 0),
    recv_bytes: Number(stream?.recv_bytes || 0),
  }));
}

function baseIdentity() {
  return {
    node_id: nodeId || null,
    node_name: nodeName,
    hostname: os.hostname(),
    agent_version: AGENT_VERSION,
    pid: process.pid,
    uptime_seconds: Math.floor(process.uptime()),
    timestamp: new Date().toISOString(),
  };
}

async function handleHealth(res) {
  const startedAt = Date.now();
  const system = getLocalSystemMetrics();
  const srs = await getSrsHealth(srsApiUrl);
  const ffmpegProcesses = getFfmpegProcessCount();

  sendJson(res, srs.healthy ? 200 : 503, {
    ok: srs.healthy,
    ...baseIdentity(),
    api_healthy: true,
    srs_healthy: srs.healthy,
    active_streams: srs.active_streams,
    ffmpeg_processes: ffmpegProcesses,
    cpu_percent: system.cpu_percent,
    memory_percent: system.memory_percent,
    disk_percent: system.disk_percent,
    load_1m: system.load_1m,
    disk_free_bytes: getDiskFreeBytes(),
    last_error: srs.error,
    response_ms: Date.now() - startedAt,
  });
}

function handleCapabilities(res) {
  sendJson(res, 200, {
    ok: true,
    ...baseIdentity(),
    mode: "read_only",
    capabilities: {
      health: true,
      system_metrics: true,
      srs_health: true,
      stream_inventory: true,
      ffmpeg_process_count: true,
      remote_job_start: false,
      remote_job_stop: false,
      stream_migration: false,
      automatic_load_balancing: false,
    },
    protocols: {
      ingest: ["rtmp", "rtmps", "srt"],
      pull_foundation: ["rtmp", "rtmps", "rtsp", "srt", "hls", "http-flv"],
      playback: ["hls"],
    },
  });
}

async function handleStreams(res) {
  try {
    const streams = await getSanitizedStreams();
    sendJson(res, 200, {
      ok: true,
      ...baseIdentity(),
      count: streams.length,
      active_count: streams.filter((stream) => stream.publish_active).length,
      streams,
    });
  } catch (error) {
    sendJson(res, 503, {
      ok: false,
      ...baseIdentity(),
      error:
        error?.name === "AbortError"
          ? "SRS stream inventory timed out"
          : error?.message || "Unable to read SRS streams",
    });
  }
}

function handleFfmpeg(res) {
  sendJson(res, 200, {
    ok: true,
    ...baseIdentity(),
    process_count: getFfmpegProcessCount(),
    note: "Phase 4A intentionally does not expose FFmpeg command lines or secrets.",
  });
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(
      req.url || "/",
      `http://${req.headers.host || "localhost"}`,
    );

    if (req.method !== "GET") {
      sendJson(res, 405, { ok: false, error: "Method not allowed" });
      return;
    }

    if (!isAuthorized(req)) {
      res.setHeader("WWW-Authenticate", 'Bearer realm="nlm-media-node-agent"');
      sendJson(res, 401, { ok: false, error: "Unauthorized" });
      return;
    }

    switch (url.pathname) {
      case "/v1/health":
        await handleHealth(res);
        return;
      case "/v1/capabilities":
        handleCapabilities(res);
        return;
      case "/v1/streams":
        await handleStreams(res);
        return;
      case "/v1/processes/ffmpeg":
        handleFfmpeg(res);
        return;
      default:
        sendJson(res, 404, { ok: false, error: "Not found" });
    }
  } catch (error) {
    console.error("[MediaNodeAgent] request failed:", error);
    if (!res.headersSent) {
      sendJson(res, 500, { ok: false, error: "Internal agent error" });
    } else {
      res.end();
    }
  }
});

server.requestTimeout = 10_000;
server.headersTimeout = 12_000;
server.keepAliveTimeout = 5_000;
server.maxRequestsPerSocket = 100;

server.on("clientError", (error, socket) => {
  console.warn("[MediaNodeAgent] client error:", error.message);
  if (socket.writable) {
    socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
  }
});

server.listen(port, host, () => {
  console.log(
    `[MediaNodeAgent] v${AGENT_VERSION} listening on http://${host}:${port} node=${nodeId || "unset"} mode=read-only`,
  );
});

function shutdown(signal) {
  console.log(`[MediaNodeAgent] ${signal} received; shutting down`);
  server.close(() => process.exit(0));

  const timer = setTimeout(() => process.exit(1), 5000);
  timer.unref?.();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
